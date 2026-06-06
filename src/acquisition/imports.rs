use std::{
    collections::{BTreeSet, HashMap, HashSet},
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use sqlx::{AnyPool, Row, any::AnyRow};
use tokio::time::MissedTickBehavior;
use uuid::Uuid;

use crate::{
    acquisition::{
        audit::{
            EVENT_MANUAL_IMPORT_COMPLETED, EVENT_MANUAL_IMPORT_QUARANTINED,
            NewAcquisitionAuditEvent, record_acquisition_audit_event,
        },
        episode_state::rebuild_library_episode_acquisition_states_for_subscription,
        release_resolution::{
            anidb::{
                AniDbChannel, AniDbChannelGateDecision, AniDbFileReconciliationInput,
                AniDbPlannedTargetIdentity, AniDbRateLimiterConfig, anidb_channel_gate_status,
                build_lookup_key, reconcile_anidb_file_identity,
            },
            hashing::{
                AnimeHashWorkerConfig, HashFileAction, HashFileJob, hash_existing_file_hash,
                queue_anime_hash_file,
            },
            models::{
                AcquisitionFileHash, AcquisitionRelease, AcquisitionReleaseCoverage,
                AcquisitionReleaseFile, AcquisitionReleaseJob, ReleaseConfidence,
                ReleaseCoverageKind, ReleaseCoverageState, ReleaseJobState, ReleaseResolverKind,
            },
            store::{get_anidb_file_cache, get_release, list_release_coverage, list_release_files},
        },
        subscriptions::{
            AcquisitionCompletionPolicy, AcquisitionMonitorPolicy, AcquisitionSubscription,
            AcquisitionTarget, AcquisitionTargetState, AcquisitionTargetStateUpdate,
            complete_terminal_acquisition_request_if_ready, get_subscription, get_target,
            list_subscription_targets, start_subscription_tracking_if_initial_download_complete,
            update_target_state,
        },
    },
    artwork::ArtworkService,
    db::models::MediaType,
    extensions::store::ExtensionStore,
    library::{
        AcquisitionLibraryImport, AcquisitionLibraryImportFile, AcquisitionLibraryImportFileResult,
        AcquisitionLibraryTargetScaffold, LinkerService,
        ingest_acquisition_library_import_with_metadata, scaffold_acquisition_library_targets,
    },
    media::ffprobe,
    metadata::MetadataService,
    runtime::RuntimePaths,
    state::AppState,
};

const IMPORT_COORDINATOR_INTERVAL_SECONDS: u64 = 30;
const IMPORT_COORDINATOR_BATCH_LIMIT: i64 = 25;

#[derive(Clone, Copy, Default)]
pub struct AcquisitionImportRuntimeServices<'a> {
    pub metadata: Option<&'a MetadataService>,
    pub linkers: Option<&'a LinkerService>,
    pub artwork: Option<&'a ArtworkService>,
}

impl<'a> AcquisitionImportRuntimeServices<'a> {
    fn from_state(state: &'a AppState) -> Self {
        Self {
            metadata: Some(state.metadata.as_ref()),
            linkers: Some(state.linkers.as_ref()),
            artwork: Some(state.artwork.as_ref()),
        }
    }
}

macro_rules! string_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $($variant:ident => $value:literal),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
        #[serde(rename_all = "snake_case")]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }
        }

        impl FromStr for $name {
            type Err = anyhow::Error;

            fn from_str(value: &str) -> Result<Self> {
                match value.trim().to_ascii_lowercase().as_str() {
                    $($value => Ok(Self::$variant),)+
                    other => bail!("unknown {} value '{other}'", stringify!($name)),
                }
            }
        }
    };
}

string_enum! {
    pub enum AcquisitionImportRunState {
        Pending => "pending",
        Blocked => "blocked",
        Importing => "importing",
        Imported => "imported",
        Mismatched => "mismatched",
        Failed => "failed",
        Cancelled => "cancelled",
    }
}

impl Default for AcquisitionImportRunState {
    fn default() -> Self {
        Self::Pending
    }
}

string_enum! {
    pub enum AcquisitionImportFileLinkState {
        Pending => "pending",
        Blocked => "blocked",
        Imported => "imported",
        Skipped => "skipped",
    }
}

impl Default for AcquisitionImportFileLinkState {
    fn default() -> Self {
        Self::Pending
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionImportRun {
    pub import_run_id: Uuid,
    pub release_id: Uuid,
    pub release_job_id: Uuid,
    pub route_logical_id: String,
    pub provider_id: Option<Uuid>,
    pub download_id: Option<String>,
    pub remote_release_id: Option<String>,
    pub state: AcquisitionImportRunState,
    pub state_reason: Option<String>,
    pub mismatch_class: Option<String>,
    pub retry_count: i64,
    pub provenance: Option<JsonValue>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewAcquisitionImportRun {
    pub import_run_id: Option<Uuid>,
    pub release_id: Uuid,
    pub release_job_id: Uuid,
    pub route_logical_id: String,
    pub provider_id: Option<Uuid>,
    pub download_id: Option<String>,
    pub remote_release_id: Option<String>,
    pub state: AcquisitionImportRunState,
    pub state_reason: Option<String>,
    pub mismatch_class: Option<String>,
    pub retry_count: i64,
    pub provenance: Option<JsonValue>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionImportFileLink {
    pub import_link_id: Uuid,
    pub import_run_id: Uuid,
    pub release_id: Uuid,
    pub release_file_id: Option<Uuid>,
    pub target_id: Option<Uuid>,
    pub local_path: Option<String>,
    pub media_file_id: Option<Uuid>,
    pub movie_id: Option<Uuid>,
    pub episode_id: Option<Uuid>,
    pub state: AcquisitionImportFileLinkState,
    pub state_reason: Option<String>,
    pub verification_state: Option<String>,
    pub mismatch_class: Option<String>,
    pub evidence: Option<JsonValue>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewAcquisitionImportFileLink {
    pub import_link_id: Option<Uuid>,
    pub import_run_id: Uuid,
    pub release_id: Uuid,
    pub release_file_id: Option<Uuid>,
    pub target_id: Option<Uuid>,
    pub local_path: Option<String>,
    pub media_file_id: Option<Uuid>,
    pub movie_id: Option<Uuid>,
    pub episode_id: Option<Uuid>,
    pub state: AcquisitionImportFileLinkState,
    pub state_reason: Option<String>,
    pub verification_state: Option<String>,
    pub mismatch_class: Option<String>,
    pub evidence: Option<JsonValue>,
}

#[derive(Debug, Clone)]
pub struct CompletedReleaseJobForImport {
    pub release: AcquisitionRelease,
    pub job: AcquisitionReleaseJob,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImportCoordinatorStats {
    pub candidates: usize,
    pub runs_created: usize,
    pub runs_reused: usize,
    pub links_upserted: usize,
    pub runs_imported: usize,
    pub links_imported: usize,
    pub blocked_runs: usize,
}

#[derive(Debug, Clone)]
pub struct AcquisitionImportPathPolicy {
    approved_roots: Vec<PathBuf>,
    allow_unrestricted: bool,
}

impl AcquisitionImportPathPolicy {
    pub fn new(approved_roots: Vec<PathBuf>) -> Self {
        Self {
            approved_roots,
            allow_unrestricted: false,
        }
    }

    #[allow(dead_code)]
    pub fn unrestricted() -> Self {
        Self {
            approved_roots: Vec::new(),
            allow_unrestricted: true,
        }
    }

    fn from_state(state: &AppState) -> Self {
        let paths = RuntimePaths::from_roots(
            &state.settings.extensions.storage_root,
            &state.settings.library.local_root,
        );
        Self::new(vec![
            PathBuf::from(paths.downloads_root),
            PathBuf::from(paths.media_root),
        ])
    }
}

pub async fn start_acquisition_import_loop(state: AppState) {
    let path_policy = AcquisitionImportPathPolicy::from_state(&state);
    let mut interval =
        tokio::time::interval(Duration::from_secs(IMPORT_COORDINATOR_INTERVAL_SECONDS));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        interval.tick().await;
        if let Err(err) = run_acquisition_import_iteration_with_services(
            &state.db_pool,
            IMPORT_COORDINATOR_BATCH_LIMIT,
            &path_policy,
            AcquisitionImportRuntimeServices::from_state(&state),
        )
        .await
        {
            tracing::warn!("acquisition import coordinator pass failed: {err}");
        }
    }
}

#[allow(dead_code)]
pub async fn run_acquisition_import_iteration(
    pool: &AnyPool,
    limit: i64,
) -> Result<ImportCoordinatorStats> {
    run_acquisition_import_iteration_with_policy(
        pool,
        limit,
        &AcquisitionImportPathPolicy::unrestricted(),
    )
    .await
}

pub async fn run_acquisition_import_iteration_with_policy(
    pool: &AnyPool,
    limit: i64,
    path_policy: &AcquisitionImportPathPolicy,
) -> Result<ImportCoordinatorStats> {
    run_acquisition_import_iteration_with_services(
        pool,
        limit,
        path_policy,
        AcquisitionImportRuntimeServices::default(),
    )
    .await
}

pub async fn run_acquisition_import_iteration_with_services(
    pool: &AnyPool,
    limit: i64,
    path_policy: &AcquisitionImportPathPolicy,
    services: AcquisitionImportRuntimeServices<'_>,
) -> Result<ImportCoordinatorStats> {
    let candidates = list_import_pending_release_jobs(pool, limit).await?;
    let mut stats = ImportCoordinatorStats {
        candidates: candidates.len(),
        ..ImportCoordinatorStats::default()
    };

    for candidate in candidates {
        let result = prepare_import_run_for_completed_job(pool, &candidate).await?;
        let blocked = result.blocked;
        let run = result.run.clone();
        if result.created {
            stats.runs_created += 1;
        } else {
            stats.runs_reused += 1;
        }
        stats.links_upserted += result.links_upserted;
        if blocked {
            stats.blocked_runs += 1;
            continue;
        }

        let finalize = finalize_import_run(pool, &candidate, &run, path_policy, services).await?;
        if finalize.imported {
            stats.runs_imported += 1;
        }
        if finalize.blocked {
            stats.blocked_runs += 1;
        }
        stats.links_imported += finalize.links_imported;
    }

    Ok(stats)
}

pub async fn list_import_pending_release_jobs(
    pool: &AnyPool,
    limit: i64,
) -> Result<Vec<CompletedReleaseJobForImport>> {
    let rows = sqlx::query(
        "SELECT
            j.release_job_id,
            j.release_id,
            j.route_logical_id,
            COALESCE(CAST(j.provider_id AS TEXT), '') AS provider_id,
            COALESCE(CAST(j.download_id AS TEXT), '') AS download_id,
            COALESCE(CAST(j.remote_release_id AS TEXT), '') AS remote_release_id,
            j.state,
            COALESCE(CAST(j.state_reason AS TEXT), '') AS state_reason,
            j.active,
            COALESCE(CAST(j.started_at AS TEXT), '') AS started_at,
            COALESCE(CAST(j.completed_at AS TEXT), '') AS completed_at,
            CAST(j.created_at AS TEXT) AS created_at,
            CAST(j.updated_at AS TEXT) AS updated_at
         FROM acquisition_release_jobs j
         JOIN acquisition_releases r ON r.release_id = j.release_id
         WHERE j.state = ?
           AND r.state = ?
           AND NOT EXISTS (
                SELECT 1
                FROM acquisition_import_runs ir
                WHERE ir.release_job_id = j.release_job_id
                  AND ir.state IN (?, ?, ?, ?, ?)
           )
         ORDER BY COALESCE(j.completed_at, j.updated_at), j.release_job_id
         LIMIT ?",
    )
    .bind(ReleaseJobState::Completed.as_str())
    .bind("completed")
    .bind(AcquisitionImportRunState::Imported.as_str())
    .bind(AcquisitionImportRunState::Blocked.as_str())
    .bind(AcquisitionImportRunState::Failed.as_str())
    .bind(AcquisitionImportRunState::Cancelled.as_str())
    .bind(AcquisitionImportRunState::Mismatched.as_str())
    .bind(limit.max(1))
    .fetch_all(pool)
    .await
    .context("listing completed release jobs for import")?;

    let mut candidates = Vec::with_capacity(rows.len());
    for row in rows {
        let job = map_release_job(&row)?;
        let release = get_release(pool, job.release_id)
            .await?
            .ok_or_else(|| anyhow!("completed release job references missing release"))?;
        candidates.push(CompletedReleaseJobForImport { release, job });
    }
    Ok(candidates)
}

#[derive(Debug, Clone)]
struct PreparedImportRun {
    run: AcquisitionImportRun,
    created: bool,
    links_upserted: usize,
    blocked: bool,
}

async fn prepare_import_run_for_completed_job(
    pool: &AnyPool,
    candidate: &CompletedReleaseJobForImport,
) -> Result<PreparedImportRun> {
    let coverage = list_release_coverage(pool, candidate.release.release_id).await?;
    let release_files = list_release_files(pool, candidate.release.release_id).await?;
    let provenance = import_run_provenance(
        &candidate.release,
        &candidate.job,
        &coverage,
        &release_files,
    );
    let (run, created) = create_or_get_import_run(
        pool,
        NewAcquisitionImportRun {
            import_run_id: None,
            release_id: candidate.release.release_id,
            release_job_id: candidate.job.release_job_id,
            route_logical_id: candidate.job.route_logical_id.clone(),
            provider_id: candidate.job.provider_id,
            download_id: candidate.job.download_id.clone(),
            remote_release_id: candidate.job.remote_release_id.clone(),
            state: AcquisitionImportRunState::Pending,
            state_reason: Some(
                "Completed release job is ready for import preparation.".to_string(),
            ),
            mismatch_class: None,
            retry_count: 0,
            provenance: Some(provenance),
            started_at: None,
            completed_at: None,
        },
    )
    .await?;

    let release_files_by_id = release_files
        .iter()
        .map(|file| (file.release_file_id, file))
        .collect::<HashMap<_, _>>();
    let selected_count = release_files
        .iter()
        .filter(|file| file.selected == Some(true))
        .count();
    let mut links_upserted = 0;
    let mut blocked_reasons = Vec::new();
    let targets_with_importable_files = coverage
        .iter()
        .filter(|entry| importable_coverage_state(entry.state))
        .filter(|entry| entry.release_file_id.is_some())
        .map(|entry| entry.target_id)
        .collect::<HashSet<_>>();

    for coverage in coverage
        .iter()
        .filter(|entry| importable_coverage_state(entry.state))
    {
        let Some(release_file_id) = coverage.release_file_id else {
            if targets_with_importable_files.contains(&coverage.target_id) {
                continue;
            }
            let reason = "selected coverage has no release file mapping";
            upsert_import_file_link(
                pool,
                NewAcquisitionImportFileLink {
                    import_link_id: None,
                    import_run_id: run.import_run_id,
                    release_id: candidate.release.release_id,
                    release_file_id: None,
                    target_id: Some(coverage.target_id),
                    local_path: None,
                    media_file_id: None,
                    movie_id: None,
                    episode_id: None,
                    state: AcquisitionImportFileLinkState::Blocked,
                    state_reason: Some(reason.to_string()),
                    verification_state: None,
                    mismatch_class: Some("missing_release_file_mapping".to_string()),
                    evidence: Some(import_link_evidence(
                        &candidate.release,
                        &candidate.job,
                        None,
                        Some(coverage.target_id),
                        None,
                        Some(coverage),
                        reason,
                    )),
                },
            )
            .await?;
            blocked_reasons.push(reason.to_string());
            links_upserted += 1;
            continue;
        };

        let Some(file) = release_files_by_id.get(&release_file_id).copied() else {
            let reason = "selected coverage references a missing release file";
            upsert_import_file_link(
                pool,
                NewAcquisitionImportFileLink {
                    import_link_id: None,
                    import_run_id: run.import_run_id,
                    release_id: candidate.release.release_id,
                    release_file_id: Some(release_file_id),
                    target_id: Some(coverage.target_id),
                    local_path: None,
                    media_file_id: None,
                    movie_id: None,
                    episode_id: None,
                    state: AcquisitionImportFileLinkState::Blocked,
                    state_reason: Some(reason.to_string()),
                    verification_state: None,
                    mismatch_class: Some("missing_release_file".to_string()),
                    evidence: Some(import_link_evidence(
                        &candidate.release,
                        &candidate.job,
                        None,
                        Some(coverage.target_id),
                        None,
                        Some(coverage),
                        reason,
                    )),
                },
            )
            .await?;
            blocked_reasons.push(reason.to_string());
            links_upserted += 1;
            continue;
        };

        if file.selected != Some(true) {
            continue;
        }

        let local_path =
            resolve_completed_file_local_path(pool, &candidate.job, file, selected_count).await?;
        let Some(local_path) = local_path else {
            let reason = "selected release file has no completed local path evidence";
            upsert_import_file_link(
                pool,
                NewAcquisitionImportFileLink {
                    import_link_id: None,
                    import_run_id: run.import_run_id,
                    release_id: candidate.release.release_id,
                    release_file_id: Some(file.release_file_id),
                    target_id: Some(coverage.target_id),
                    local_path: None,
                    media_file_id: None,
                    movie_id: None,
                    episode_id: None,
                    state: AcquisitionImportFileLinkState::Blocked,
                    state_reason: Some(reason.to_string()),
                    verification_state: None,
                    mismatch_class: Some("missing_local_path".to_string()),
                    evidence: Some(import_link_evidence(
                        &candidate.release,
                        &candidate.job,
                        Some(file),
                        Some(coverage.target_id),
                        None,
                        Some(coverage),
                        reason,
                    )),
                },
            )
            .await?;
            blocked_reasons.push(reason.to_string());
            links_upserted += 1;
            continue;
        };

        let exists = local_media_file_exists(&local_path).await;
        let (state, reason, mismatch_class) = if exists {
            (
                AcquisitionImportFileLinkState::Pending,
                "Selected file is ready for RR-8B library linking.".to_string(),
                None,
            )
        } else {
            (
                AcquisitionImportFileLinkState::Blocked,
                "completed local file is missing from disk".to_string(),
                Some("missing_local_file".to_string()),
            )
        };
        if !exists {
            blocked_reasons.push(reason.clone());
        }
        upsert_import_file_link(
            pool,
            NewAcquisitionImportFileLink {
                import_link_id: None,
                import_run_id: run.import_run_id,
                release_id: candidate.release.release_id,
                release_file_id: Some(file.release_file_id),
                target_id: Some(coverage.target_id),
                local_path: Some(local_path.clone()),
                media_file_id: None,
                movie_id: None,
                episode_id: None,
                state,
                state_reason: Some(reason.clone()),
                verification_state: None,
                mismatch_class,
                evidence: Some(import_link_evidence(
                    &candidate.release,
                    &candidate.job,
                    Some(file),
                    Some(coverage.target_id),
                    Some(&local_path),
                    Some(coverage),
                    &reason,
                )),
            },
        )
        .await?;
        links_upserted += 1;
    }

    let (state, reason) = if !blocked_reasons.is_empty() {
        (
            AcquisitionImportRunState::Blocked,
            format!(
                "Import preparation blocked: {}.",
                blocked_reasons
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "release file evidence is incomplete".to_string())
            ),
        )
    } else if links_upserted == 0 {
        (
            AcquisitionImportRunState::Blocked,
            "Import preparation blocked: no selected/submitted coverage rows were importable."
                .to_string(),
        )
    } else {
        (
            AcquisitionImportRunState::Pending,
            "Completed release job is ready for RR-8B library linking.".to_string(),
        )
    };
    transition_import_run_state(pool, run.import_run_id, state, &reason, None).await?;

    Ok(PreparedImportRun {
        run,
        created,
        links_upserted,
        blocked: state == AcquisitionImportRunState::Blocked,
    })
}

#[derive(Debug, Clone, Default)]
struct FinalizedImportRun {
    imported: bool,
    blocked: bool,
    links_imported: usize,
}

#[derive(Debug, Clone)]
struct AnimeImportVerificationPass {
    links: Vec<AcquisitionImportFileLink>,
    finalized: Option<FinalizedImportRun>,
}

#[derive(Debug, Clone)]
struct MediaSafetyVerificationPass {
    links: Vec<AcquisitionImportFileLink>,
    finalized: Option<FinalizedImportRun>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AnimeImportFileGroupKey {
    release_file_id: Uuid,
    local_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ImportFileGroupKey {
    release_file_id: Uuid,
    local_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ImportAudioPolicy {
    required_languages: Vec<String>,
    sources: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ImportAudioDecision {
    Pass,
    Block {
        verification_state: &'static str,
        mismatch_class: &'static str,
        reason: String,
    },
}

async fn finalize_import_run(
    pool: &AnyPool,
    candidate: &CompletedReleaseJobForImport,
    run: &AcquisitionImportRun,
    path_policy: &AcquisitionImportPathPolicy,
    services: AcquisitionImportRuntimeServices<'_>,
) -> Result<FinalizedImportRun> {
    if run.state == AcquisitionImportRunState::Imported {
        return Ok(FinalizedImportRun {
            imported: false,
            blocked: false,
            links_imported: 0,
        });
    }

    transition_import_run_state(
        pool,
        run.import_run_id,
        AcquisitionImportRunState::Importing,
        "Importing completed acquisition files into the library.",
        None,
    )
    .await?;

    let result = finalize_import_run_inner(pool, candidate, run, path_policy, services).await;
    match result {
        Ok(finalized) => Ok(finalized),
        Err(err) => {
            transition_import_run_state(
                pool,
                run.import_run_id,
                AcquisitionImportRunState::Failed,
                &format!("Library import failed: {err}"),
                Some("library_import_failed"),
            )
            .await?;
            Err(err)
        }
    }
}

async fn finalize_import_run_inner(
    pool: &AnyPool,
    candidate: &CompletedReleaseJobForImport,
    run: &AcquisitionImportRun,
    path_policy: &AcquisitionImportPathPolicy,
    services: AcquisitionImportRuntimeServices<'_>,
) -> Result<FinalizedImportRun> {
    let Some(subscription_id) = candidate.release.subscription_id else {
        block_import_run(
            pool,
            run.import_run_id,
            "release is not tied to an acquisition subscription",
            Some("missing_subscription"),
        )
        .await?;
        return Ok(FinalizedImportRun {
            blocked: true,
            ..FinalizedImportRun::default()
        });
    };
    let Some(subscription) = get_subscription(pool, subscription_id).await? else {
        block_import_run(
            pool,
            run.import_run_id,
            "release references a missing acquisition subscription",
            Some("missing_subscription"),
        )
        .await?;
        return Ok(FinalizedImportRun {
            blocked: true,
            ..FinalizedImportRun::default()
        });
    };

    let links = list_import_file_links(pool, run.import_run_id).await?;
    let mut importable_links: Vec<_> = links
        .iter()
        .filter(|link| link.state == AcquisitionImportFileLinkState::Pending)
        .cloned()
        .collect();
    let is_movie_import = subscription.media_type == MediaType::Movie;
    if importable_links.is_empty() {
        if !links.is_empty()
            && links
                .iter()
                .all(|link| link.state == AcquisitionImportFileLinkState::Imported)
        {
            mark_release_and_job_imported(pool, candidate.release.release_id, run.release_job_id)
                .await?;
            update_subscription_completion_if_ready(pool, &subscription).await?;
            transition_import_run_state(
                pool,
                run.import_run_id,
                AcquisitionImportRunState::Imported,
                "Imported completed acquisition files into the library.",
                None,
            )
            .await?;
            record_manual_import_completed_if_applicable(pool, &candidate.release, run, 0).await?;
            return Ok(FinalizedImportRun {
                imported: true,
                blocked: false,
                links_imported: 0,
            });
        }
        block_import_run(
            pool,
            run.import_run_id,
            "import run has no pending file links to import",
            Some("missing_import_links"),
        )
        .await?;
        return Ok(FinalizedImportRun {
            blocked: true,
            ..FinalizedImportRun::default()
        });
    }
    if is_movie_import && importable_links.len() != 1 {
        return block_import_run_result(
            pool,
            run.import_run_id,
            "movie import requires exactly one selected coverage file",
            Some("movie_selected_file_count"),
        )
        .await;
    }

    let release_files = list_release_files(pool, candidate.release.release_id).await?;
    let release_files_by_id = release_files
        .iter()
        .map(|file| (file.release_file_id, file))
        .collect::<HashMap<_, _>>();
    let coverage = list_release_coverage(pool, candidate.release.release_id).await?;
    let mut coverage_by_file_target = HashMap::new();
    for coverage in &coverage {
        if let Some(release_file_id) = coverage.release_file_id {
            coverage_by_file_target.insert((release_file_id, coverage.target_id), coverage);
        }
    }

    let mut target_ids = HashSet::new();
    for link in &importable_links {
        let Some(target_id) = link.target_id else {
            return block_import_run_result(
                pool,
                run.import_run_id,
                "import file link is missing a target id",
                Some("missing_target"),
            )
            .await;
        };
        let Some(release_file_id) = link.release_file_id else {
            return block_import_run_result(
                pool,
                run.import_run_id,
                "import file link is missing a release file id",
                Some("missing_release_file"),
            )
            .await;
        };
        let Some(release_file) = release_files_by_id.get(&release_file_id).copied() else {
            return block_import_run_result(
                pool,
                run.import_run_id,
                "import file link references a missing release file",
                Some("missing_release_file"),
            )
            .await;
        };
        if release_file.selected != Some(true) {
            return block_import_run_result(
                pool,
                run.import_run_id,
                "import file link references an unselected release file",
                Some("unselected_release_file"),
            )
            .await;
        }
        let Some(coverage) = coverage_by_file_target
            .get(&(release_file_id, target_id))
            .copied()
        else {
            return block_import_run_result(
                pool,
                run.import_run_id,
                "import file link has no matching release coverage row",
                Some("missing_coverage"),
            )
            .await;
        };
        if !importable_coverage_state(coverage.state) {
            return block_import_run_result(
                pool,
                run.import_run_id,
                "import file link coverage is no longer selected or submitted",
                Some("coverage_not_importable"),
            )
            .await;
        }
        if is_movie_import && !movie_import_coverage_kind_allowed(coverage.coverage_kind) {
            return block_import_run_result(
                pool,
                run.import_run_id,
                "movie import coverage must use movie or manual override coverage",
                Some("invalid_movie_coverage_kind"),
            )
            .await;
        }
        if !target_ids.insert(target_id) {
            return block_import_run_result(
                pool,
                run.import_run_id,
                "one target is mapped more than once in the same import run",
                Some("duplicate_target_mapping"),
            )
            .await;
        }
    }

    let mut targets_by_id = HashMap::new();
    for target_id in target_ids {
        let Some(target) = get_target(pool, target_id).await? else {
            return block_import_run_result(
                pool,
                run.import_run_id,
                "import coverage references a missing acquisition target",
                Some("missing_target"),
            )
            .await;
        };
        if target.subscription_id != subscription.subscription_id {
            return block_import_run_result(
                pool,
                run.import_run_id,
                "import target belongs to a different acquisition subscription",
                Some("subscription_mismatch"),
            )
            .await;
        }
        if target.state == AcquisitionTargetState::Imported
            && target.import_event_id != Some(run.import_run_id)
        {
            return block_import_run_result(
                pool,
                run.import_run_id,
                "target is already imported by another release",
                Some("target_already_imported"),
            )
            .await;
        }
        targets_by_id.insert(target.target_id, target);
    }

    let verification = verify_anime_import_links(
        pool,
        &subscription,
        &candidate.release,
        run,
        importable_links,
        &targets_by_id,
        &coverage_by_file_target,
    )
    .await?;
    if let Some(finalized) = verification.finalized {
        return Ok(finalized);
    }
    importable_links = verification.links;

    let media_safety = verify_import_media_safety(
        pool,
        &subscription,
        &candidate.release,
        run,
        importable_links,
        &release_files_by_id,
    )
    .await?;
    if let Some(finalized) = media_safety.finalized {
        return Ok(finalized);
    }
    importable_links = media_safety.links;

    let mut library_files = Vec::new();
    for link in &importable_links {
        let local_path = link
            .local_path
            .as_deref()
            .ok_or_else(|| anyhow!("pending import link is missing local path"))?;
        if let Err(err) = validate_import_local_path(local_path, path_policy).await {
            return block_import_run_result(
                pool,
                run.import_run_id,
                &format!("import path validation failed: {err}"),
                Some("invalid_import_path"),
            )
            .await;
        }
        let target = targets_by_id
            .get(&link.target_id.expect("target checked"))
            .ok_or_else(|| anyhow!("validated target was not loaded"))?;
        let release_file = link
            .release_file_id
            .and_then(|id| release_files_by_id.get(&id).copied());
        library_files.push(library_import_file_from_link(
            local_path,
            release_file,
            target,
        )?);
    }

    let request = build_library_import_request(&subscription, &candidate.release, library_files)?;
    let import_result = ingest_acquisition_library_import_with_metadata(
        pool,
        services.metadata,
        services.linkers,
        services.artwork,
        request,
    )
    .await?;
    ExtensionStore::new(pool)
        .upsert_acquisition_media_ownership(
            import_result.media_item_id,
            subscription.subscription_id,
            candidate.release.source_provider_id,
            Some(&candidate.release.source_extension_id),
        )
        .await?;
    scaffold_subscription_targets_into_library(
        pool,
        services.artwork,
        &subscription,
        import_result.media_item_id,
    )
    .await?;
    let result_by_key = import_result
        .files
        .iter()
        .map(|result| (library_result_key(result), result))
        .collect::<HashMap<_, _>>();

    let mut links_imported = 0;
    for link in importable_links {
        let target = targets_by_id
            .get(&link.target_id.expect("target checked"))
            .ok_or_else(|| anyhow!("validated target was not loaded"))?;
        let result_key = import_result_key_for_target(
            link.local_path
                .as_deref()
                .ok_or_else(|| anyhow!("pending import link is missing local path"))?,
            target,
        );
        let Some(result) = result_by_key.get(&result_key).copied() else {
            return block_import_run_result(
                pool,
                run.import_run_id,
                "library import did not return a link for one selected target",
                Some("missing_library_link"),
            )
            .await;
        };
        if let Some(episode_id) = result.episode_id {
            apply_target_episode_metadata(pool, episode_id, target).await?;
        }
        mark_import_file_link_imported(pool, &link, result, run).await?;
        mark_release_coverage_imported(
            pool,
            candidate.release.release_id,
            link.release_file_id.expect("release file checked"),
            target.target_id,
        )
        .await?;
        update_target_state(
            pool,
            target.target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Imported,
                state_reason: Some("Imported into the Elixir library.".to_string()),
                selected_provider_id: run.provider_id,
                selected_route_logical_id: Some(run.route_logical_id.clone()),
                selected_candidate: target.selected_candidate.clone(),
                download_id: run.download_id.clone(),
                import_event_id: Some(run.import_run_id),
                next_search_after: target.next_search_after,
                increment_search_attempts: false,
            },
        )
        .await?;
        links_imported += 1;
    }

    mark_release_and_job_imported(pool, candidate.release.release_id, run.release_job_id).await?;
    start_subscription_tracking_if_initial_download_complete(
        pool,
        subscription.subscription_id,
        Utc::now(),
    )
    .await?;
    update_subscription_completion_if_ready(pool, &subscription).await?;
    transition_import_run_state(
        pool,
        run.import_run_id,
        AcquisitionImportRunState::Imported,
        "Imported completed acquisition files into the library.",
        None,
    )
    .await?;
    record_manual_import_completed_if_applicable(pool, &candidate.release, run, links_imported)
        .await?;

    Ok(FinalizedImportRun {
        imported: true,
        blocked: false,
        links_imported,
    })
}

async fn scaffold_subscription_targets_into_library(
    pool: &AnyPool,
    artwork: Option<&ArtworkService>,
    subscription: &AcquisitionSubscription,
    media_item_id: Uuid,
) -> Result<()> {
    if !matches!(
        subscription.media_type,
        MediaType::Series | MediaType::Anime
    ) {
        return Ok(());
    }

    let targets = list_subscription_targets(pool, subscription.subscription_id).await?;
    let scaffolds = targets
        .iter()
        .map(|target| AcquisitionLibraryTargetScaffold {
            media_type: target.media_type,
            title: target.title.clone(),
            season_number: target.season_number,
            episode_number: target.episode_number,
            absolute_episode_number: target.absolute_episode_number,
            metadata: target.metadata.clone(),
        })
        .collect::<Vec<_>>();
    let scaffolded =
        scaffold_acquisition_library_targets(pool, artwork, media_item_id, &scaffolds).await?;
    if scaffolded > 0 {
        rebuild_library_episode_acquisition_states_for_subscription(
            pool,
            subscription.subscription_id,
        )
        .await?;
    }
    Ok(())
}

async fn block_import_run_result(
    pool: &AnyPool,
    import_run_id: Uuid,
    reason: &str,
    mismatch_class: Option<&str>,
) -> Result<FinalizedImportRun> {
    block_import_run(pool, import_run_id, reason, mismatch_class).await?;
    Ok(FinalizedImportRun {
        blocked: true,
        ..FinalizedImportRun::default()
    })
}

async fn block_import_run(
    pool: &AnyPool,
    import_run_id: Uuid,
    reason: &str,
    mismatch_class: Option<&str>,
) -> Result<()> {
    transition_import_run_state(
        pool,
        import_run_id,
        AcquisitionImportRunState::Blocked,
        reason,
        mismatch_class,
    )
    .await?;
    Ok(())
}

async fn verify_anime_import_links(
    pool: &AnyPool,
    subscription: &AcquisitionSubscription,
    release: &AcquisitionRelease,
    run: &AcquisitionImportRun,
    importable_links: Vec<AcquisitionImportFileLink>,
    targets_by_id: &HashMap<Uuid, AcquisitionTarget>,
    coverage_by_file_target: &HashMap<(Uuid, Uuid), &AcquisitionReleaseCoverage>,
) -> Result<AnimeImportVerificationPass> {
    if subscription.media_type != MediaType::Anime {
        return Ok(AnimeImportVerificationPass {
            links: importable_links,
            finalized: None,
        });
    }

    let mut links_by_id = importable_links
        .iter()
        .cloned()
        .map(|link| (link.import_link_id, link))
        .collect::<HashMap<_, _>>();
    let mut groups: HashMap<AnimeImportFileGroupKey, Vec<AcquisitionImportFileLink>> =
        HashMap::new();
    for link in &importable_links {
        let Some(release_file_id) = link.release_file_id else {
            continue;
        };
        let Some(local_path) = link.local_path.as_deref() else {
            continue;
        };
        groups
            .entry(AnimeImportFileGroupKey {
                release_file_id,
                local_path: local_path.to_string(),
            })
            .or_default()
            .push(link.clone());
    }

    let explicit_verification_override =
        anime_import_allows_manual_verification_override(release.coverage_plan.as_ref());
    let require_hash = anime_import_requires_hash_verification(release.coverage_plan.as_ref());
    for (group_key, group_links) in groups {
        let all_links_are_manual_override = group_links.iter().all(|link| {
            link.target_id
                .and_then(|target_id| {
                    coverage_by_file_target
                        .get(&(group_key.release_file_id, target_id))
                        .map(|coverage| {
                            coverage.coverage_kind == ReleaseCoverageKind::ManualOverride
                        })
                })
                .unwrap_or(false)
        });
        if explicit_verification_override && all_links_are_manual_override {
            for link in mark_anime_group_verification(
                pool,
                run,
                &group_links,
                "manual_override",
                "Anime post-download verification bypassed by explicit user-approved mapping.",
                None,
                json!({
                    "phase": "rr8c",
                    "verificationState": "manual_override",
                    "explicitVerificationOverride": explicit_verification_override,
                    "releaseFileId": group_key.release_file_id,
                    "localPath": group_key.local_path,
                    "previousEvidence": group_links.iter().map(|link| link.evidence.clone()).collect::<Vec<_>>(),
                }),
                AcquisitionImportFileLinkState::Pending,
            )
            .await?
            {
                links_by_id.insert(link.import_link_id, link);
            }
            continue;
        }

        let hash = match ensure_anime_import_file_hash(
            pool,
            group_key.release_file_id,
            &group_key.local_path,
        )
        .await
        {
            Ok(hash) => hash,
            Err(err) => {
                let reason = format!("Anime hash verification failed: {err}");
                mark_anime_group_verification(
                    pool,
                    run,
                    &group_links,
                    "hash_failed",
                    &reason,
                    Some("anime_hash_failed"),
                    json!({
                        "phase": "rr8c",
                        "verificationState": "hash_failed",
                        "releaseFileId": group_key.release_file_id,
                        "localPath": group_key.local_path,
                        "error": err.to_string(),
                    }),
                    AcquisitionImportFileLinkState::Blocked,
                )
                .await?;
                block_import_run(pool, run.import_run_id, &reason, Some("anime_hash_failed"))
                    .await?;
                return Ok(AnimeImportVerificationPass {
                    links: links_by_id.into_values().collect(),
                    finalized: Some(FinalizedImportRun {
                        blocked: true,
                        ..FinalizedImportRun::default()
                    }),
                });
            }
        };

        let Some(ed2k) = hash.ed2k.clone() else {
            let reason = "Anime hash verification is pending an ED2K digest.";
            mark_anime_group_verification(
                pool,
                run,
                &group_links,
                "hash_pending",
                reason,
                Some("anime_hash_pending"),
                anime_hash_evidence("hash_pending", &group_key, &hash, None),
                AcquisitionImportFileLinkState::Blocked,
            )
            .await?;
            block_import_run(pool, run.import_run_id, reason, Some("anime_hash_pending")).await?;
            return Ok(AnimeImportVerificationPass {
                links: links_by_id.into_values().collect(),
                finalized: Some(FinalizedImportRun {
                    blocked: true,
                    ..FinalizedImportRun::default()
                }),
            });
        };
        let lookup_key = build_lookup_key(&ed2k, hash.size_bytes);
        let planned_targets =
            anime_planned_targets_for_links(subscription, &group_links, targets_by_id);

        if let Some(cache) = get_anidb_file_cache(pool, &lookup_key).await? {
            let reconciliation = reconcile_anidb_file_identity(
                pool,
                AniDbFileReconciliationInput {
                    lookup_key: lookup_key.clone(),
                    release_id: Some(release.release_id),
                    release_file_id: Some(group_key.release_file_id),
                    candidate_fingerprint: Some(release.fingerprint.clone()),
                    planned_targets: planned_targets.clone(),
                },
            )
            .await?;
            let evidence = json!({
                "phase": "rr8c",
                "verificationState": reconciliation.outcome.as_str(),
                "lookupKey": reconciliation.lookup_key,
                "ed2k": ed2k,
                "sizeBytes": hash.size_bytes,
                "cacheStatus": cache.lookup_status.as_str(),
                "cacheHit": true,
                "matchAttemptId": reconciliation.match_attempt.match_attempt_id,
                "reviewReasons": reconciliation.review_reasons,
                "xrefs": reconciliation.xrefs,
                "mismatchIds": reconciliation
                    .mismatches
                    .iter()
                    .map(|mismatch| mismatch.mismatch_id)
                    .collect::<Vec<_>>(),
                "plannedTargets": planned_targets,
            });
            match reconciliation.outcome.as_str() {
                "verified" => {
                    for link in mark_anime_group_verification(
                        pool,
                        run,
                        &group_links,
                        "verified",
                        "Anime file identity verified by ED2K/AniDB before import.",
                        None,
                        evidence,
                        AcquisitionImportFileLinkState::Pending,
                    )
                    .await?
                    {
                        links_by_id.insert(link.import_link_id, link);
                    }
                }
                "mismatch" | "no_match" => {
                    let mismatch_class = if reconciliation.outcome.as_str() == "no_match" {
                        "anime_hash_no_match"
                    } else {
                        "anime_hash_identity_mismatch"
                    };
                    let reason =
                        "Anime post-download hash identity disagrees with the planned target.";
                    mark_anime_group_verification(
                        pool,
                        run,
                        &group_links,
                        reconciliation.outcome.as_str(),
                        reason,
                        Some(mismatch_class),
                        evidence,
                        AcquisitionImportFileLinkState::Blocked,
                    )
                    .await?;
                    quarantine_anime_import_mismatch(
                        pool,
                        release.release_id,
                        run.import_run_id,
                        reason,
                        mismatch_class,
                    )
                    .await?;
                    return Ok(AnimeImportVerificationPass {
                        links: links_by_id.into_values().collect(),
                        finalized: Some(FinalizedImportRun {
                            blocked: true,
                            ..FinalizedImportRun::default()
                        }),
                    });
                }
                _ => {
                    let reason = "Anime hash identity could not be reconciled safely yet.";
                    mark_anime_group_verification(
                        pool,
                        run,
                        &group_links,
                        "deferred",
                        reason,
                        Some("anime_hash_reconciliation_deferred"),
                        evidence,
                        AcquisitionImportFileLinkState::Blocked,
                    )
                    .await?;
                    block_import_run(
                        pool,
                        run.import_run_id,
                        reason,
                        Some("anime_hash_reconciliation_deferred"),
                    )
                    .await?;
                    return Ok(AnimeImportVerificationPass {
                        links: links_by_id.into_values().collect(),
                        finalized: Some(FinalizedImportRun {
                            blocked: true,
                            ..FinalizedImportRun::default()
                        }),
                    });
                }
            }
            continue;
        }

        let gate = anidb_channel_gate_status(
            pool,
            AniDbChannel::Udp,
            &AniDbRateLimiterConfig::default(),
            Utc::now(),
        )
        .await?;
        if require_hash || !anime_release_can_trust_pre_download_mapping(release) {
            let (reason, mismatch_class) = anidb_lookup_deferred_reason(&gate);
            mark_anime_group_verification(
                pool,
                run,
                &group_links,
                "deferred",
                &reason,
                Some(&mismatch_class),
                json!({
                    "phase": "rr8c",
                    "verificationState": "deferred",
                    "lookupKey": lookup_key,
                    "ed2k": ed2k,
                    "sizeBytes": hash.size_bytes,
                    "cacheHit": false,
                    "gate": gate,
                    "requireHashBeforeImport": require_hash,
                    "plannedTargets": planned_targets,
                }),
                AcquisitionImportFileLinkState::Blocked,
            )
            .await?;
            block_import_run(pool, run.import_run_id, &reason, Some(&mismatch_class)).await?;
            return Ok(AnimeImportVerificationPass {
                links: links_by_id.into_values().collect(),
                finalized: Some(FinalizedImportRun {
                    blocked: true,
                    ..FinalizedImportRun::default()
                }),
            });
        }

        for link in mark_anime_group_verification(
            pool,
            run,
            &group_links,
            "trusted_pre_download_mapping",
            "Anime pre-download resolver mapping is high confidence; hash identity is unavailable.",
            None,
            json!({
                "phase": "rr8c",
                "verificationState": "trusted_pre_download_mapping",
                "lookupKey": lookup_key,
                "ed2k": ed2k,
                "sizeBytes": hash.size_bytes,
                "cacheHit": false,
                "gate": gate,
                "requireHashBeforeImport": require_hash,
                "plannedTargets": planned_targets,
            }),
            AcquisitionImportFileLinkState::Pending,
        )
        .await?
        {
            links_by_id.insert(link.import_link_id, link);
        }
    }

    let links = importable_links
        .into_iter()
        .filter_map(|link| links_by_id.remove(&link.import_link_id))
        .collect();
    Ok(AnimeImportVerificationPass {
        links,
        finalized: None,
    })
}

async fn verify_import_media_safety(
    pool: &AnyPool,
    subscription: &AcquisitionSubscription,
    release: &AcquisitionRelease,
    run: &AcquisitionImportRun,
    importable_links: Vec<AcquisitionImportFileLink>,
    release_files_by_id: &HashMap<Uuid, &AcquisitionReleaseFile>,
) -> Result<MediaSafetyVerificationPass> {
    if subscription.media_type == MediaType::Anime {
        return Ok(MediaSafetyVerificationPass {
            links: importable_links,
            finalized: None,
        });
    }

    let mut links_by_id = importable_links
        .iter()
        .cloned()
        .map(|link| (link.import_link_id, link))
        .collect::<HashMap<_, _>>();
    let mut groups: HashMap<ImportFileGroupKey, Vec<AcquisitionImportFileLink>> = HashMap::new();
    for link in &importable_links {
        let Some(release_file_id) = link.release_file_id else {
            continue;
        };
        let Some(local_path) = link.local_path.as_deref() else {
            continue;
        };
        groups
            .entry(ImportFileGroupKey {
                release_file_id,
                local_path: local_path.to_string(),
            })
            .or_default()
            .push(link.clone());
    }

    for (group_key, group_links) in groups {
        let Some(release_file) = release_files_by_id.get(&group_key.release_file_id).copied()
        else {
            continue;
        };
        let policy = import_audio_policy(subscription, release, release_file);
        if policy.required_languages.is_empty() {
            continue;
        }

        let metadata = match ffprobe::probe(&group_key.local_path).await {
            Ok(metadata) => metadata,
            Err(err) => {
                tracing::warn!(
                    "audio language verification skipped for import run {} release file {}: {err}",
                    run.import_run_id,
                    group_key.release_file_id
                );
                continue;
            }
        };
        let evidence = import_audio_safety_evidence(
            "checked",
            &policy,
            &metadata,
            release_file,
            &group_key.local_path,
        );
        let decision = evaluate_import_audio_policy(&metadata, &policy);
        if let ImportAudioDecision::Block {
            verification_state,
            mismatch_class,
            reason,
        } = decision
        {
            let updated_links = mark_import_media_safety_group(
                pool,
                run,
                &group_links,
                verification_state,
                &reason,
                Some(mismatch_class),
                import_audio_safety_evidence(
                    verification_state,
                    &policy,
                    &metadata,
                    release_file,
                    &group_key.local_path,
                ),
                AcquisitionImportFileLinkState::Blocked,
            )
            .await?;
            for link in updated_links {
                links_by_id.insert(link.import_link_id, link);
            }
            block_import_run(pool, run.import_run_id, &reason, Some(mismatch_class)).await?;
            return Ok(MediaSafetyVerificationPass {
                links: links_by_id.into_values().collect(),
                finalized: Some(FinalizedImportRun {
                    blocked: true,
                    ..FinalizedImportRun::default()
                }),
            });
        }

        for link in mark_import_media_safety_group(
            pool,
            run,
            &group_links,
            "verified",
            "Import audio language evidence satisfied the selected release policy.",
            None,
            evidence,
            AcquisitionImportFileLinkState::Pending,
        )
        .await?
        {
            links_by_id.insert(link.import_link_id, link);
        }
    }

    let links = importable_links
        .into_iter()
        .filter_map(|link| links_by_id.remove(&link.import_link_id))
        .collect();
    Ok(MediaSafetyVerificationPass {
        links,
        finalized: None,
    })
}

async fn mark_import_media_safety_group(
    pool: &AnyPool,
    run: &AcquisitionImportRun,
    links: &[AcquisitionImportFileLink],
    verification_state: &str,
    reason: &str,
    mismatch_class: Option<&str>,
    evidence: JsonValue,
    state: AcquisitionImportFileLinkState,
) -> Result<Vec<AcquisitionImportFileLink>> {
    let mut updated = Vec::with_capacity(links.len());
    for link in links {
        updated.push(
            upsert_import_file_link(
                pool,
                NewAcquisitionImportFileLink {
                    import_link_id: Some(link.import_link_id),
                    import_run_id: link.import_run_id,
                    release_id: link.release_id,
                    release_file_id: link.release_file_id,
                    target_id: link.target_id,
                    local_path: link.local_path.clone(),
                    media_file_id: link.media_file_id,
                    movie_id: link.movie_id,
                    episode_id: link.episode_id,
                    state,
                    state_reason: Some(reason.to_string()),
                    verification_state: Some(verification_state.to_string()),
                    mismatch_class: mismatch_class.map(ToString::to_string),
                    evidence: Some(json!({
                        "phase": "rr8d",
                        "importRunId": run.import_run_id,
                        "releaseId": link.release_id,
                        "releaseFileId": link.release_file_id,
                        "targetId": link.target_id,
                        "localPath": link.local_path,
                        "verification": evidence.clone(),
                        "previousEvidence": link.evidence,
                    })),
                },
            )
            .await?,
        );
    }
    Ok(updated)
}

fn import_audio_policy(
    subscription: &AcquisitionSubscription,
    release: &AcquisitionRelease,
    release_file: &AcquisitionReleaseFile,
) -> ImportAudioPolicy {
    let mut required = BTreeSet::new();
    let mut sources = BTreeSet::new();

    for key in [
        "requiredAudioLanguages",
        "required_audio_languages",
        "audioLanguages",
        "audio_languages",
        "requiredLanguages",
        "required_languages",
        "languages",
        "language",
    ] {
        add_policy_languages_from_json_path(
            subscription.quality_profile.as_ref(),
            &[key],
            &format!("qualityProfile.{key}"),
            &mut required,
            &mut sources,
        );
    }
    for path in [
        &["audio", "requiredLanguages"][..],
        &["audio", "required_languages"][..],
        &["languageProfile", "languages"][..],
    ] {
        add_policy_languages_from_json_path(
            subscription.quality_profile.as_ref(),
            path,
            &format!("qualityProfile.{}", path.join(".")),
            &mut required,
            &mut sources,
        );
    }

    for path in [
        &["language"][..],
        &["languages"][..],
        &["audioLanguage"][..],
        &["audioLanguages"][..],
        &["parsedLanguage"][..],
        &["raw", "language"][..],
        &["raw", "languages"][..],
        &["raw", "audioLanguage"][..],
        &["raw", "audioLanguages"][..],
    ] {
        add_policy_languages_from_json_path(
            release.selected_candidate.as_ref(),
            path,
            &format!("selectedCandidate.{}", path.join(".")),
            &mut required,
            &mut sources,
        );
    }

    if let Some(language) = release_file.parsed_language.as_deref() {
        add_normalized_import_language(
            language,
            "releaseFile.parsedLanguage",
            &mut required,
            &mut sources,
        );
    }

    ImportAudioPolicy {
        required_languages: required.into_iter().collect(),
        sources: sources.into_iter().collect(),
    }
}

fn add_policy_languages_from_json_path(
    root: Option<&JsonValue>,
    path: &[&str],
    source: &str,
    required: &mut BTreeSet<String>,
    sources: &mut BTreeSet<String>,
) {
    let Some(value) = json_value_path(root, path) else {
        return;
    };
    for raw in json_language_values(value) {
        add_normalized_import_language(&raw, source, required, sources);
    }
}

fn json_value_path<'a>(root: Option<&'a JsonValue>, path: &[&str]) -> Option<&'a JsonValue> {
    let mut cursor = root?;
    for key in path {
        cursor = cursor.get(*key)?;
    }
    Some(cursor)
}

fn json_language_values(value: &JsonValue) -> Vec<String> {
    if let Some(raw) = value.as_str() {
        return split_language_list(raw);
    }
    if let Some(values) = value.as_array() {
        return values
            .iter()
            .flat_map(json_language_values)
            .collect::<Vec<_>>();
    }
    if let Some(object) = value.as_object() {
        return ["language", "name", "value", "code", "id"]
            .iter()
            .filter_map(|key| object.get(*key))
            .flat_map(json_language_values)
            .collect();
    }
    Vec::new()
}

fn split_language_list(raw: &str) -> Vec<String> {
    raw.split(|ch: char| matches!(ch, ',' | ';' | '/' | '|'))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn add_normalized_import_language(
    raw: &str,
    source: &str,
    required: &mut BTreeSet<String>,
    sources: &mut BTreeSet<String>,
) {
    let Some(language) = normalize_import_language_value(raw) else {
        return;
    };
    required.insert(language);
    sources.insert(source.to_string());
}

fn evaluate_import_audio_policy(
    metadata: &ffprobe::MediaMetadata,
    policy: &ImportAudioPolicy,
) -> ImportAudioDecision {
    if policy.required_languages.is_empty() {
        return ImportAudioDecision::Pass;
    }
    let audio_stream_count = import_audio_streams(metadata).count();
    if audio_stream_count == 0 {
        return ImportAudioDecision::Block {
            verification_state: "audio_missing",
            mismatch_class: "media_audio_missing",
            reason: "Downloaded file has no audio streams to satisfy the selected release policy."
                .to_string(),
        };
    }
    let detected = detected_audio_languages(metadata);
    if detected.is_empty() {
        return ImportAudioDecision::Pass;
    }
    if policy
        .required_languages
        .iter()
        .any(|required| detected.contains(required))
    {
        return ImportAudioDecision::Pass;
    }
    ImportAudioDecision::Block {
        verification_state: "audio_language_mismatch",
        mismatch_class: "audio_language_mismatch",
        reason: format!(
            "Downloaded file audio languages {} do not include required or claimed language {}.",
            format_language_list(detected.iter()),
            format_language_list(policy.required_languages.iter())
        ),
    }
}

fn import_audio_safety_evidence(
    verification_state: &str,
    policy: &ImportAudioPolicy,
    metadata: &ffprobe::MediaMetadata,
    release_file: &AcquisitionReleaseFile,
    local_path: &str,
) -> JsonValue {
    json!({
        "phase": "rr8d",
        "verificationState": verification_state,
        "localPath": local_path,
        "releaseFileId": release_file.release_file_id,
        "releaseFilePath": release_file.path.as_str(),
        "parsedLanguage": release_file.parsed_language.as_deref(),
        "requiredAudioLanguages": &policy.required_languages,
        "policySources": &policy.sources,
        "detectedAudioLanguages": detected_audio_languages(metadata),
        "container": metadata.container.as_deref(),
        "videoCodec": metadata.video_codec.as_deref(),
        "audioCodec": metadata.audio_codec.as_deref(),
        "durationSeconds": metadata.duration_seconds,
        "audioStreams": import_audio_streams(metadata)
            .map(import_audio_stream_evidence)
            .collect::<Vec<_>>(),
    })
}

fn import_audio_streams(
    metadata: &ffprobe::MediaMetadata,
) -> impl Iterator<Item = &ffprobe::Stream> {
    metadata
        .streams
        .iter()
        .filter(|stream| stream.codec_type.as_deref() == Some("audio"))
}

fn import_audio_stream_evidence(stream: &ffprobe::Stream) -> JsonValue {
    json!({
        "index": stream.index,
        "codec": stream.codec_name.as_deref(),
        "channels": stream.channels,
        "language": stream_language_tag(stream),
        "normalizedLanguage": stream_normalized_audio_language(stream),
        "title": stream_text_tag(stream, "title"),
        "handlerName": stream_text_tag(stream, "handler_name"),
        "default": stream
            .disposition
            .as_ref()
            .and_then(|disposition| disposition.default_flag)
            .unwrap_or_default()
            == 1,
        "forced": stream
            .disposition
            .as_ref()
            .and_then(|disposition| disposition.forced)
            .unwrap_or_default()
            == 1,
    })
}

fn detected_audio_languages(metadata: &ffprobe::MediaMetadata) -> Vec<String> {
    import_audio_streams(metadata)
        .filter_map(stream_normalized_audio_language)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn stream_normalized_audio_language(stream: &ffprobe::Stream) -> Option<String> {
    stream_language_tag(stream)
        .as_deref()
        .and_then(normalize_import_language_value)
        .or_else(|| {
            stream_text_tag(stream, "title")
                .as_deref()
                .and_then(normalize_import_language_freeform)
        })
        .or_else(|| {
            stream_text_tag(stream, "handler_name")
                .as_deref()
                .and_then(normalize_import_language_freeform)
        })
}

fn stream_language_tag(stream: &ffprobe::Stream) -> Option<String> {
    stream_text_tag(stream, "language")
        .or_else(|| stream_text_tag(stream, "LANGUAGE"))
        .or_else(|| stream_text_tag(stream, "lang"))
}

fn stream_text_tag(stream: &ffprobe::Stream, key: &str) -> Option<String> {
    stream
        .tags
        .as_ref()
        .and_then(|tags| {
            tags.iter()
                .find(|(tag_key, _)| tag_key.eq_ignore_ascii_case(key))
                .map(|(_, value)| value.clone())
        })
        .and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
}

fn normalize_import_language_freeform(raw: &str) -> Option<String> {
    normalize_import_language_value(raw).or_else(|| {
        raw.split(|ch: char| !ch.is_ascii_alphanumeric())
            .filter_map(normalize_import_language_name_or_three_letter)
            .next()
    })
}

fn normalize_import_language_value(raw: &str) -> Option<String> {
    let value = raw.trim();
    if value.is_empty() {
        return None;
    }
    let normalized = value.replace('_', "-");
    let first = normalized
        .split('-')
        .find(|part| !part.trim().is_empty())?
        .trim()
        .to_ascii_lowercase();
    normalize_import_language_token(&first)
}

fn normalize_import_language_name_or_three_letter(token: &str) -> Option<String> {
    let token = token.trim().to_ascii_lowercase();
    if token.len() == 3 && token.chars().all(|ch| ch.is_ascii_alphabetic()) {
        return map_import_three_letter_language(&token).map(ToString::to_string);
    }
    map_import_language_name(&token).map(ToString::to_string)
}

fn normalize_import_language_token(token: &str) -> Option<String> {
    let token = token.trim().to_ascii_lowercase();
    if matches!(
        token.as_str(),
        "" | "und" | "undefined" | "unknown" | "unk" | "mul" | "multi" | "dual" | "original"
    ) {
        return None;
    }
    if token.len() == 2 && token.chars().all(|ch| ch.is_ascii_alphabetic()) {
        return Some(token);
    }
    if token.len() == 3 && token.chars().all(|ch| ch.is_ascii_alphabetic()) {
        return map_import_three_letter_language(&token)
            .map(ToString::to_string)
            .or(Some(token));
    }
    map_import_language_name(&token).map(ToString::to_string)
}

fn map_import_three_letter_language(token: &str) -> Option<&'static str> {
    match token {
        "eng" => Some("en"),
        "spa" => Some("es"),
        "fra" | "fre" => Some("fr"),
        "deu" | "ger" => Some("de"),
        "ita" => Some("it"),
        "por" => Some("pt"),
        "nld" | "dut" => Some("nl"),
        "rus" => Some("ru"),
        "jpn" => Some("ja"),
        "kor" => Some("ko"),
        "zho" | "chi" => Some("zh"),
        "ara" => Some("ar"),
        "heb" => Some("he"),
        "hin" => Some("hi"),
        "tur" => Some("tr"),
        "pol" => Some("pl"),
        "ukr" => Some("uk"),
        "swe" => Some("sv"),
        "fin" => Some("fi"),
        "dan" => Some("da"),
        "nor" => Some("no"),
        "ron" | "rum" => Some("ro"),
        "ell" | "gre" => Some("el"),
        "ces" | "cze" => Some("cs"),
        "hun" => Some("hu"),
        "tha" => Some("th"),
        "vie" => Some("vi"),
        "ind" => Some("id"),
        "msa" | "may" => Some("ms"),
        "fas" | "per" => Some("fa"),
        "urd" => Some("ur"),
        "tam" => Some("ta"),
        "tel" => Some("te"),
        "ben" => Some("bn"),
        "mar" => Some("mr"),
        "lit" => Some("lt"),
        "lav" => Some("lv"),
        "est" => Some("et"),
        "slv" => Some("sl"),
        "slk" | "slo" => Some("sk"),
        "hrv" => Some("hr"),
        "srp" => Some("sr"),
        "bul" => Some("bg"),
        "isl" | "ice" => Some("is"),
        "gle" => Some("ga"),
        "kat" | "geo" => Some("ka"),
        "kaz" => Some("kk"),
        "tgl" => Some("tl"),
        _ => None,
    }
}

fn map_import_language_name(token: &str) -> Option<&'static str> {
    match token {
        "english" => Some("en"),
        "spanish" | "espanol" | "castellano" => Some("es"),
        "french" => Some("fr"),
        "german" => Some("de"),
        "italian" => Some("it"),
        "portuguese" => Some("pt"),
        "russian" => Some("ru"),
        "japanese" => Some("ja"),
        "korean" => Some("ko"),
        "chinese" | "mandarin" | "cantonese" => Some("zh"),
        "dutch" => Some("nl"),
        "swedish" => Some("sv"),
        "norwegian" => Some("no"),
        "danish" => Some("da"),
        "finnish" => Some("fi"),
        "polish" => Some("pl"),
        "turkish" => Some("tr"),
        "arabic" => Some("ar"),
        "hebrew" => Some("he"),
        "greek" => Some("el"),
        "czech" => Some("cs"),
        "hungarian" => Some("hu"),
        "thai" => Some("th"),
        "vietnamese" => Some("vi"),
        "indonesian" => Some("id"),
        "malay" => Some("ms"),
        "persian" => Some("fa"),
        "hindi" => Some("hi"),
        "ukrainian" => Some("uk"),
        "romanian" => Some("ro"),
        "latino" => Some("es"),
        _ => None,
    }
}

fn format_language_list<'a>(languages: impl Iterator<Item = &'a String>) -> String {
    let values = languages.cloned().collect::<Vec<_>>();
    if values.is_empty() {
        "[]".to_string()
    } else {
        values.join(", ")
    }
}

async fn ensure_anime_import_file_hash(
    pool: &AnyPool,
    release_file_id: Uuid,
    local_path: &str,
) -> Result<AcquisitionFileHash> {
    let queued = queue_anime_hash_file(
        pool,
        HashFileJob {
            release_file_id: Some(release_file_id),
            local_file_id: Some(format!("release-file:{release_file_id}")),
            file_path: PathBuf::from(local_path),
            force_rehash: false,
        },
    )
    .await?;
    match queued.action {
        HashFileAction::Reused | HashFileAction::Hashed => Ok(queued.file_hash),
        HashFileAction::Queued | HashFileAction::Failed => {
            Ok(
                hash_existing_file_hash(pool, queued.file_hash, &AnimeHashWorkerConfig::default())
                    .await?
                    .file_hash,
            )
        }
    }
}

async fn mark_anime_group_verification(
    pool: &AnyPool,
    run: &AcquisitionImportRun,
    links: &[AcquisitionImportFileLink],
    verification_state: &str,
    reason: &str,
    mismatch_class: Option<&str>,
    evidence: JsonValue,
    state: AcquisitionImportFileLinkState,
) -> Result<Vec<AcquisitionImportFileLink>> {
    let mut updated = Vec::with_capacity(links.len());
    for link in links {
        updated.push(
            upsert_import_file_link(
                pool,
                NewAcquisitionImportFileLink {
                    import_link_id: Some(link.import_link_id),
                    import_run_id: link.import_run_id,
                    release_id: link.release_id,
                    release_file_id: link.release_file_id,
                    target_id: link.target_id,
                    local_path: link.local_path.clone(),
                    media_file_id: link.media_file_id,
                    movie_id: link.movie_id,
                    episode_id: link.episode_id,
                    state,
                    state_reason: Some(reason.to_string()),
                    verification_state: Some(verification_state.to_string()),
                    mismatch_class: mismatch_class.map(ToString::to_string),
                    evidence: Some(json!({
                        "phase": "rr8c",
                        "importRunId": run.import_run_id,
                        "releaseId": link.release_id,
                        "releaseFileId": link.release_file_id,
                        "targetId": link.target_id,
                        "localPath": link.local_path,
                        "verification": evidence.clone(),
                        "previousEvidence": link.evidence,
                    })),
                },
            )
            .await?,
        );
    }
    Ok(updated)
}

async fn quarantine_anime_import_mismatch(
    pool: &AnyPool,
    release_id: Uuid,
    import_run_id: Uuid,
    reason: &str,
    mismatch_class: &str,
) -> Result<()> {
    transition_import_run_state(
        pool,
        import_run_id,
        AcquisitionImportRunState::Mismatched,
        reason,
        Some(mismatch_class),
    )
    .await?;
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET state = ?,
             state_reason = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = ?",
    )
    .bind("review_required")
    .bind(reason)
    .bind(release_id.to_string())
    .execute(pool)
    .await
    .context("quarantining mismatched anime acquisition release")?;
    if let Some(release) = get_release(pool, release_id).await?
        && release_has_manual_review_approval(&release)
    {
        record_acquisition_audit_event(
            pool,
            NewAcquisitionAuditEvent {
                event_type: EVENT_MANUAL_IMPORT_QUARANTINED.to_string(),
                release_id: Some(release_id),
                subscription_id: release.subscription_id,
                import_run_id: Some(import_run_id),
                state: Some(AcquisitionImportRunState::Mismatched.as_str().to_string()),
                reason: Some(reason.to_string()),
                evidence: Some(json!({
                    "releaseFingerprint": release.fingerprint,
                    "mismatchClass": mismatch_class,
                    "manualReview": release.coverage_plan.as_ref().and_then(|plan| plan.get("manualReview")),
                    "animeVerification": release.coverage_plan.as_ref().and_then(|plan| plan.get("animeVerification")),
                })),
                ..NewAcquisitionAuditEvent::default()
            },
        )
        .await?;
    }
    Ok(())
}

async fn record_manual_import_completed_if_applicable(
    pool: &AnyPool,
    release: &AcquisitionRelease,
    run: &AcquisitionImportRun,
    links_imported: usize,
) -> Result<()> {
    if !release_has_manual_review_approval(release) {
        return Ok(());
    }
    record_acquisition_audit_event(
        pool,
        NewAcquisitionAuditEvent {
            event_type: EVENT_MANUAL_IMPORT_COMPLETED.to_string(),
            release_id: Some(release.release_id),
            subscription_id: release.subscription_id,
            release_job_id: Some(run.release_job_id),
            import_run_id: Some(run.import_run_id),
            state: Some(AcquisitionImportRunState::Imported.as_str().to_string()),
            reason: Some("Manual acquisition review import completed.".to_string()),
            evidence: Some(json!({
                "releaseFingerprint": release.fingerprint,
                "routeLogicalId": run.route_logical_id,
                "providerId": run.provider_id,
                "downloadId": run.download_id,
                "remoteReleaseId": run.remote_release_id,
                "linksImported": links_imported,
                "manualReview": release.coverage_plan.as_ref().and_then(|plan| plan.get("manualReview")),
                "provenance": run.provenance.clone(),
            })),
            ..NewAcquisitionAuditEvent::default()
        },
    )
    .await?;
    Ok(())
}

fn release_has_manual_review_approval(release: &AcquisitionRelease) -> bool {
    json_bool_path(
        release.coverage_plan.as_ref(),
        &["manualReview", "userApproved"],
    ) == Some(true)
        || json_bool_path(
            release.coverage_plan.as_ref(),
            &["priorityPolicy", "userApproved"],
        ) == Some(true)
}

fn anime_planned_targets_for_links(
    subscription: &AcquisitionSubscription,
    links: &[AcquisitionImportFileLink],
    targets_by_id: &HashMap<Uuid, AcquisitionTarget>,
) -> Vec<AniDbPlannedTargetIdentity> {
    links
        .iter()
        .filter_map(|link| {
            link.target_id
                .and_then(|target_id| targets_by_id.get(&target_id))
        })
        .map(|target| {
            let metadata = target.metadata.as_ref();
            AniDbPlannedTargetIdentity {
                target_id: Some(target.target_id),
                target_key: Some(target.target_key.clone()),
                title: Some(target.title.clone()).filter(|title| !title.trim().is_empty()),
                season_number: target.season_number,
                episode_number: target.episode_number,
                absolute_episode_number: target.absolute_episode_number,
                anidb_anime_id: metadata_i64(metadata, &["anidbAnimeId", "anidbId"]).or_else(
                    || {
                        subscription
                            .external_ids
                            .as_ref()
                            .and_then(|ids| ids.anidb.as_deref())
                            .and_then(parse_i64_string)
                    },
                ),
                anidb_episode_id: metadata_i64(
                    metadata,
                    &["anidbEpisodeId", "anidbEpisodeID", "anidbEid"],
                ),
            }
        })
        .collect()
}

fn anime_hash_evidence(
    verification_state: &str,
    group_key: &AnimeImportFileGroupKey,
    hash: &AcquisitionFileHash,
    extra: Option<JsonValue>,
) -> JsonValue {
    json!({
        "phase": "rr8c",
        "verificationState": verification_state,
        "releaseFileId": group_key.release_file_id,
        "localPath": group_key.local_path,
        "fileHashId": hash.file_hash_id,
        "ed2k": hash.ed2k,
        "crc32": hash.crc32,
        "sizeBytes": hash.size_bytes,
        "hashStatus": hash.hash_status.as_str(),
        "extra": extra,
    })
}

fn anime_import_allows_manual_verification_override(plan: Option<&JsonValue>) -> bool {
    json_bool_path(plan, &["animeVerification", "userApprovedImportOverride"]) == Some(true)
        || json_bool_path(
            plan,
            &["animeVerification", "manualImportVerificationOverride"],
        ) == Some(true)
}

fn anime_import_requires_hash_verification(plan: Option<&JsonValue>) -> bool {
    json_bool_path(plan, &["animeVerification", "requireHashBeforeImport"]) == Some(true)
        || json_bool_path(plan, &["animeVerification", "requireVerifiedHash"]) == Some(true)
}

fn anime_release_can_trust_pre_download_mapping(release: &AcquisitionRelease) -> bool {
    release.resolver_kind == ReleaseResolverKind::AnimeShokoStyle
        && release.confidence == ReleaseConfidence::High
}

fn anidb_lookup_deferred_reason(gate: &AniDbChannelGateDecision) -> (String, String) {
    match gate {
        AniDbChannelGateDecision::RateLimited { retry_after, .. } => (
            format!(
                "AniDB FILE lookup is rate-limited until {}.",
                retry_after.to_rfc3339()
            ),
            "anidb_lookup_rate_limited".to_string(),
        ),
        AniDbChannelGateDecision::Banned {
            retry_after,
            reason,
            ..
        } => (
            format!(
                "AniDB FILE lookup is banned until {}{}.",
                retry_after.to_rfc3339(),
                reason
                    .as_deref()
                    .map(|value| format!(": {value}"))
                    .unwrap_or_default()
            ),
            "anidb_lookup_banned".to_string(),
        ),
        AniDbChannelGateDecision::BackingOff {
            retry_after,
            reason,
            ..
        } => (
            format!(
                "AniDB FILE lookup is backing off until {}{}.",
                retry_after.to_rfc3339(),
                reason
                    .as_deref()
                    .map(|value| format!(": {value}"))
                    .unwrap_or_default()
            ),
            "anidb_lookup_backoff".to_string(),
        ),
        AniDbChannelGateDecision::Disabled { reason, .. } => (
            format!("AniDB FILE lookup is disabled: {reason}."),
            "anidb_lookup_disabled".to_string(),
        ),
        AniDbChannelGateDecision::Allowed { .. } => (
            "AniDB FILE cache is missing for the verified local hash.".to_string(),
            "anidb_file_cache_missing".to_string(),
        ),
    }
}

fn metadata_i64(metadata: Option<&JsonValue>, keys: &[&str]) -> Option<i64> {
    keys.iter().find_map(|key| {
        metadata
            .and_then(|value| value.get(*key))
            .and_then(json_i64_or_string)
    })
}

fn json_i64_or_string(value: &JsonValue) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(parse_i64_string))
}

fn parse_i64_string(value: &str) -> Option<i64> {
    value.trim().parse::<i64>().ok()
}

fn json_bool_path(value: Option<&JsonValue>, path: &[&str]) -> Option<bool> {
    let mut cursor = value?;
    for key in path {
        cursor = cursor.get(*key)?;
    }
    cursor.as_bool()
}

fn build_library_import_request(
    subscription: &AcquisitionSubscription,
    _release: &AcquisitionRelease,
    files: Vec<AcquisitionLibraryImportFile>,
) -> Result<AcquisitionLibraryImport> {
    let media_type = subscription.media_type;
    if media_type == MediaType::Movie && files.len() != 1 {
        bail!("movie acquisition import requires exactly one selected file");
    }
    Ok(AcquisitionLibraryImport {
        media_type,
        title: subscription.title.clone(),
        year: subscription.year,
        external_ids: subscription.external_ids.clone().unwrap_or_default(),
        files: files
            .into_iter()
            .map(|mut file| {
                if media_type == MediaType::Movie {
                    file.season_number = None;
                    file.episode_number = None;
                    file.absolute_episode_number = None;
                    file.episode_title = None;
                }
                file
            })
            .collect(),
    })
}

fn library_import_file_from_link(
    local_path: &str,
    release_file: Option<&AcquisitionReleaseFile>,
    target: &AcquisitionTarget,
) -> Result<AcquisitionLibraryImportFile> {
    let size_bytes = release_file.and_then(|file| file.size_bytes);
    Ok(AcquisitionLibraryImportFile {
        path: local_path.to_string(),
        size_bytes,
        season_number: target.season_number,
        episode_number: target.episode_number,
        absolute_episode_number: target.absolute_episode_number,
        episode_title: Some(target.title.clone()).filter(|title| !title.trim().is_empty()),
    })
}

async fn apply_target_episode_metadata(
    pool: &AnyPool,
    episode_id: Uuid,
    target: &AcquisitionTarget,
) -> Result<()> {
    let Some(metadata) = target.metadata.as_ref() else {
        return Ok(());
    };
    let episode_metadata = metadata
        .get("raw")
        .cloned()
        .unwrap_or_else(|| metadata.clone());
    let metadata_json = serde_json::to_string(&episode_metadata).ok();
    let runtime_seconds = target_episode_runtime_seconds(&episode_metadata);
    let title = episode_metadata
        .get("name")
        .or_else(|| episode_metadata.get("title"))
        .and_then(JsonValue::as_str)
        .or(Some(target.title.as_str()))
        .map(str::trim)
        .filter(|value| !value.is_empty());

    sqlx::query::<sqlx::Any>(
        "UPDATE episodes
         SET title = COALESCE(?, NULLIF(title, ''), title),
             runtime_seconds = COALESCE(?, runtime_seconds),
             metadata_json = COALESCE(?, NULLIF(TRIM(CAST(metadata_json AS TEXT)), ''), metadata_json),
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ?",
    )
    .bind(title)
    .bind(runtime_seconds)
    .bind(metadata_json)
    .bind(episode_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

fn target_episode_runtime_seconds(metadata: &JsonValue) -> Option<i32> {
    let direct_seconds = metadata
        .get("runtimeSeconds")
        .or_else(|| metadata.get("durationSeconds"))
        .and_then(JsonValue::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .filter(|value| *value > 0);
    if direct_seconds.is_some() {
        return direct_seconds;
    }

    metadata
        .get("runtime")
        .or_else(|| metadata.get("duration"))
        .and_then(JsonValue::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .filter(|value| *value > 0)
        .map(|value| if value < 1_000 { value * 60 } else { value })
}

fn library_result_key(result: &AcquisitionLibraryImportFileResult) -> String {
    format!(
        "{}|{}|{}",
        normalize_path_key(&result.path),
        result.season_number.unwrap_or_default(),
        result.episode_number.unwrap_or_default()
    )
}

fn import_result_key_for_target(local_path: &str, target: &AcquisitionTarget) -> String {
    format!(
        "{}|{}|{}",
        normalize_path_key(local_path),
        target.season_number.unwrap_or_default(),
        target.episode_number.unwrap_or_default()
    )
}

fn normalize_path_key(path: &str) -> String {
    Path::new(path)
        .to_string_lossy()
        .trim()
        .to_ascii_lowercase()
}

async fn validate_import_local_path(
    path: &str,
    policy: &AcquisitionImportPathPolicy,
) -> Result<()> {
    let metadata = tokio::fs::metadata(path)
        .await
        .with_context(|| format!("import file '{path}' is not readable"))?;
    if !metadata.is_file() {
        bail!("import path is not a regular file");
    }
    if !is_supported_import_media_file(path) {
        bail!("import path is not a supported media file");
    }
    if is_sample_or_extra_import_path(path) {
        bail!("import path appears to be a sample or extra");
    }
    if policy.allow_unrestricted {
        return Ok(());
    }
    let canonical_path = tokio::fs::canonicalize(path).await?;
    let mut canonical_roots = Vec::new();
    for root in &policy.approved_roots {
        if let Ok(root) = tokio::fs::canonicalize(root).await {
            canonical_roots.push(root);
        }
    }
    if canonical_roots.is_empty() {
        bail!("no approved import roots are available");
    }
    if canonical_roots
        .iter()
        .any(|root| canonical_path.starts_with(root))
    {
        return Ok(());
    }
    bail!("import path is outside approved download or media roots")
}

fn is_supported_import_media_file(path: &str) -> bool {
    let Some(ext) = Path::new(path)
        .extension()
        .map(|value| value.to_string_lossy().to_ascii_lowercase())
    else {
        return false;
    };
    matches!(
        ext.as_str(),
        "mkv" | "mp4" | "m4v" | "avi" | "mov" | "wmv" | "ts" | "m2ts" | "webm"
    )
}

fn is_sample_or_extra_import_path(path: &str) -> bool {
    let normalized = path.replace('\\', "/").to_ascii_lowercase();
    let basename = Path::new(path)
        .file_name()
        .map(|value| value.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_default();
    basename.contains("sample")
        || normalized.contains("/sample")
        || normalized.contains("/samples/")
        || normalized.contains("/extras/")
}

async fn mark_import_file_link_imported(
    pool: &AnyPool,
    link: &AcquisitionImportFileLink,
    result: &AcquisitionLibraryImportFileResult,
    run: &AcquisitionImportRun,
) -> Result<()> {
    upsert_import_file_link(
        pool,
        NewAcquisitionImportFileLink {
            import_link_id: Some(link.import_link_id),
            import_run_id: link.import_run_id,
            release_id: link.release_id,
            release_file_id: link.release_file_id,
            target_id: link.target_id,
            local_path: link.local_path.clone(),
            media_file_id: Some(result.media_file_id),
            movie_id: result.movie_id,
            episode_id: result.episode_id,
            state: AcquisitionImportFileLinkState::Imported,
            state_reason: Some("Linked into the Elixir library.".to_string()),
            verification_state: link.verification_state.clone(),
            mismatch_class: None,
            evidence: Some(json!({
                "phase": "rr8b",
                "importRunId": run.import_run_id,
                "releaseId": link.release_id,
                "releaseFileId": link.release_file_id,
                "targetId": link.target_id,
                "localPath": link.local_path,
                "mediaFileId": result.media_file_id,
                "movieId": result.movie_id,
                "episodeId": result.episode_id,
                "routeLogicalId": run.route_logical_id,
                "downloadId": run.download_id,
                "remoteReleaseId": run.remote_release_id,
                "previousEvidence": link.evidence,
            })),
        },
    )
    .await?;
    Ok(())
}

async fn mark_release_coverage_imported(
    pool: &AnyPool,
    release_id: Uuid,
    release_file_id: Uuid,
    target_id: Uuid,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_coverage
         SET state = ?,
             reason = ?,
             verified_by = COALESCE(verified_by, ?),
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = ?
           AND release_file_id = ?
           AND target_id = ?",
    )
    .bind(ReleaseCoverageState::Imported.as_str())
    .bind("Imported into the Elixir library.")
    .bind("rr8b_library_import")
    .bind(release_id.to_string())
    .bind(release_file_id.to_string())
    .bind(target_id.to_string())
    .execute(pool)
    .await
    .context("marking release coverage imported")?;
    Ok(())
}

async fn mark_release_and_job_imported(
    pool: &AnyPool,
    release_id: Uuid,
    release_job_id: Uuid,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET state_reason = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = ?",
    )
    .bind("Imported into the Elixir library.")
    .bind(release_id.to_string())
    .execute(pool)
    .await
    .context("marking acquisition release imported")?;

    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_jobs
         SET state_reason = ?,
             active = 0,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_job_id = ?",
    )
    .bind("Imported into the Elixir library.")
    .bind(release_job_id.to_string())
    .execute(pool)
    .await
    .context("marking acquisition release job imported")?;
    Ok(())
}

async fn update_subscription_completion_if_ready(
    pool: &AnyPool,
    subscription: &AcquisitionSubscription,
) -> Result<()> {
    if subscription.completion_policy == AcquisitionCompletionPolicy::TerminalSelectedTargets {
        complete_terminal_acquisition_request_if_ready(
            pool,
            subscription.subscription_id,
            "All scoped acquisition targets were imported or excluded.",
        )
        .await?;
        return Ok(());
    }

    let remaining: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM acquisition_targets
         WHERE subscription_id = ?
           AND state NOT IN ('imported', 'excluded')",
    )
    .bind(subscription.subscription_id.to_string())
    .fetch_one(pool)
    .await
    .context("checking acquisition subscription completion")?;

    if remaining != 0 {
        return Ok(());
    }

    let should_complete = subscription.media_type == MediaType::Movie
        || matches!(
            subscription.monitor_policy,
            AcquisitionMonitorPolicy::SelectedTargets | AcquisitionMonitorPolicy::SelectedSeasons
        );
    if !should_complete {
        return Ok(());
    }

    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_subscriptions
         SET status = 'completed',
             active = 0,
             updated_at = CURRENT_TIMESTAMP
         WHERE subscription_id = ?",
    )
    .bind(subscription.subscription_id.to_string())
    .execute(pool)
    .await
    .context("marking acquisition subscription completed")?;
    Ok(())
}

fn importable_coverage_state(state: ReleaseCoverageState) -> bool {
    matches!(
        state,
        ReleaseCoverageState::Selected | ReleaseCoverageState::Submitted
    )
}

fn movie_import_coverage_kind_allowed(kind: ReleaseCoverageKind) -> bool {
    matches!(
        kind,
        ReleaseCoverageKind::Movie | ReleaseCoverageKind::ManualOverride
    )
}

async fn resolve_completed_file_local_path(
    pool: &AnyPool,
    job: &AcquisitionReleaseJob,
    file: &AcquisitionReleaseFile,
    selected_count: usize,
) -> Result<Option<String>> {
    if let Some(path) = metadata_local_path(file.provider_metadata.as_ref())
        .or_else(|| metadata_local_path(file.raw.as_ref()))
    {
        return Ok(Some(path));
    }
    let Some(job_path) = lookup_debrid_job_local_path(pool, job).await? else {
        return Ok(None);
    };
    Ok(infer_file_path_from_job_path(
        &job_path,
        file,
        selected_count,
    ))
}

fn metadata_local_path(value: Option<&JsonValue>) -> Option<String> {
    let value = value?;
    ["localPath", "local_path", "absolutePath", "path"]
        .into_iter()
        .find_map(|key| {
            value
                .get(key)
                .and_then(JsonValue::as_str)
                .map(str::trim)
                .filter(|path| !path.is_empty())
                .map(str::to_string)
        })
}

async fn lookup_debrid_job_local_path(
    pool: &AnyPool,
    job: &AcquisitionReleaseJob,
) -> Result<Option<String>> {
    let download_id = job.download_id.as_deref().unwrap_or_default();
    let remote_release_id = job.remote_release_id.as_deref().unwrap_or_default();
    if download_id.is_empty() && remote_release_id.is_empty() {
        return Ok(None);
    }
    let row = sqlx::query(
        "SELECT COALESCE(CAST(local_path AS TEXT), '') AS local_path
         FROM debrid_download_jobs
         WHERE job_id = ?
            OR remote_release_id = ?
            OR remote_torrent_id = ?
            OR remote_download_id = ?
         ORDER BY updated_at DESC
         LIMIT 1",
    )
    .bind(download_id)
    .bind(remote_release_id)
    .bind(remote_release_id)
    .bind(remote_release_id)
    .fetch_optional(pool)
    .await
    .context("looking up completed debrid job local path")?;
    let Some(row) = row else {
        return Ok(None);
    };
    Ok(row_get_opt_string(&row, "local_path")?.filter(|path| !path.trim().is_empty()))
}

fn infer_file_path_from_job_path(
    job_path: &str,
    file: &AcquisitionReleaseFile,
    selected_count: usize,
) -> Option<String> {
    let trimmed = job_path.trim();
    if trimmed.is_empty() {
        return None;
    }
    let path = Path::new(trimmed);
    if selected_count == 1 && path.extension().is_some() {
        return Some(trimmed.to_string());
    }
    Some(path.join(&file.basename).to_string_lossy().to_string())
}

async fn local_media_file_exists(path: &str) -> bool {
    tokio::fs::metadata(path)
        .await
        .map(|metadata| metadata.is_file())
        .unwrap_or(false)
}

pub async fn create_or_get_import_run(
    pool: &AnyPool,
    data: NewAcquisitionImportRun,
) -> Result<(AcquisitionImportRun, bool)> {
    validate_import_run_input(&data)?;
    if let Some(existing) = get_import_run_by_release_job(pool, data.release_job_id).await? {
        return Ok((existing, false));
    }
    let import_run_id = data.import_run_id.unwrap_or_else(Uuid::new_v4);
    let provenance_json = json_to_string(data.provenance.as_ref())?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO acquisition_import_runs (
            import_run_id,
            release_id,
            release_job_id,
            route_logical_id,
            provider_id,
            download_id,
            remote_release_id,
            state,
            state_reason,
            mismatch_class,
            retry_count,
            provenance_json,
            started_at,
            completed_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(import_run_id.to_string())
    .bind(data.release_id.to_string())
    .bind(data.release_job_id.to_string())
    .bind(data.route_logical_id.trim())
    .bind(data.provider_id.map(|value| value.to_string()))
    .bind(data.download_id.as_deref())
    .bind(data.remote_release_id.as_deref())
    .bind(data.state.as_str())
    .bind(data.state_reason.as_deref())
    .bind(data.mismatch_class.as_deref())
    .bind(data.retry_count)
    .bind(provenance_json.as_deref())
    .bind(data.started_at.map(db_datetime_string))
    .bind(data.completed_at.map(db_datetime_string))
    .execute(pool)
    .await
    .context("creating acquisition import run")?;

    get_import_run(pool, import_run_id)
        .await?
        .map(|run| (run, true))
        .ok_or_else(|| anyhow!("created acquisition import run was not readable"))
}

pub async fn get_import_run_by_release_job(
    pool: &AnyPool,
    release_job_id: Uuid,
) -> Result<Option<AcquisitionImportRun>> {
    let row = sqlx::query(IMPORT_RUN_SELECT_BY_RELEASE_JOB)
        .bind(release_job_id.to_string())
        .fetch_optional(pool)
        .await?;
    row.map(|row| map_import_run(&row)).transpose()
}

pub async fn get_import_run(
    pool: &AnyPool,
    import_run_id: Uuid,
) -> Result<Option<AcquisitionImportRun>> {
    let row = sqlx::query(IMPORT_RUN_SELECT_BY_ID)
        .bind(import_run_id.to_string())
        .fetch_optional(pool)
        .await?;
    row.map(|row| map_import_run(&row)).transpose()
}

pub async fn transition_import_run_state(
    pool: &AnyPool,
    import_run_id: Uuid,
    state: AcquisitionImportRunState,
    reason: &str,
    mismatch_class: Option<&str>,
) -> Result<Option<AcquisitionImportRun>> {
    let completed_at = matches!(
        state,
        AcquisitionImportRunState::Imported
            | AcquisitionImportRunState::Failed
            | AcquisitionImportRunState::Cancelled
            | AcquisitionImportRunState::Mismatched
    )
    .then(db_datetime_string_now);
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_import_runs
         SET state = ?,
             state_reason = ?,
             mismatch_class = COALESCE(?, mismatch_class),
             completed_at = COALESCE(?, completed_at),
             updated_at = CURRENT_TIMESTAMP
         WHERE import_run_id = ?",
    )
    .bind(state.as_str())
    .bind(reason)
    .bind(mismatch_class)
    .bind(completed_at.as_deref())
    .bind(import_run_id.to_string())
    .execute(pool)
    .await
    .context("transitioning acquisition import run state")?;

    get_import_run(pool, import_run_id).await
}

pub async fn upsert_import_file_link(
    pool: &AnyPool,
    data: NewAcquisitionImportFileLink,
) -> Result<AcquisitionImportFileLink> {
    validate_import_file_link_input(&data)?;
    let existing = find_import_file_link(pool, &data).await?;
    let import_link_id = existing
        .as_ref()
        .map(|link| link.import_link_id)
        .or(data.import_link_id)
        .unwrap_or_else(Uuid::new_v4);
    let evidence_json = json_to_string(data.evidence.as_ref())?;

    if existing.is_some() {
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_import_file_links
             SET release_id = ?,
                 release_file_id = ?,
                 target_id = ?,
                 local_path = ?,
                 media_file_id = ?,
                 movie_id = ?,
                 episode_id = ?,
                 state = ?,
                 state_reason = ?,
                 verification_state = ?,
                 mismatch_class = ?,
                 evidence_json = ?,
                 updated_at = CURRENT_TIMESTAMP
             WHERE import_link_id = ?",
        )
        .bind(data.release_id.to_string())
        .bind(data.release_file_id.map(|value| value.to_string()))
        .bind(data.target_id.map(|value| value.to_string()))
        .bind(data.local_path.as_deref())
        .bind(data.media_file_id.map(|value| value.to_string()))
        .bind(data.movie_id.map(|value| value.to_string()))
        .bind(data.episode_id.map(|value| value.to_string()))
        .bind(data.state.as_str())
        .bind(data.state_reason.as_deref())
        .bind(data.verification_state.as_deref())
        .bind(data.mismatch_class.as_deref())
        .bind(evidence_json.as_deref())
        .bind(import_link_id.to_string())
        .execute(pool)
        .await
        .context("updating acquisition import file link")?;
    } else {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_import_file_links (
                import_link_id,
                import_run_id,
                release_id,
                release_file_id,
                target_id,
                local_path,
                media_file_id,
                movie_id,
                episode_id,
                state,
                state_reason,
                verification_state,
                mismatch_class,
                evidence_json
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(import_link_id.to_string())
        .bind(data.import_run_id.to_string())
        .bind(data.release_id.to_string())
        .bind(data.release_file_id.map(|value| value.to_string()))
        .bind(data.target_id.map(|value| value.to_string()))
        .bind(data.local_path.as_deref())
        .bind(data.media_file_id.map(|value| value.to_string()))
        .bind(data.movie_id.map(|value| value.to_string()))
        .bind(data.episode_id.map(|value| value.to_string()))
        .bind(data.state.as_str())
        .bind(data.state_reason.as_deref())
        .bind(data.verification_state.as_deref())
        .bind(data.mismatch_class.as_deref())
        .bind(evidence_json.as_deref())
        .execute(pool)
        .await
        .context("creating acquisition import file link")?;
    }

    get_import_file_link(pool, import_link_id)
        .await?
        .ok_or_else(|| anyhow!("upserted acquisition import file link was not readable"))
}

pub async fn list_import_file_links(
    pool: &AnyPool,
    import_run_id: Uuid,
) -> Result<Vec<AcquisitionImportFileLink>> {
    let rows = sqlx::query(IMPORT_FILE_LINK_SELECT_BY_RUN)
        .bind(import_run_id.to_string())
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|row| map_import_file_link(&row))
        .collect()
}

pub async fn list_import_runs_by_release(
    pool: &AnyPool,
    release_id: Uuid,
) -> Result<Vec<AcquisitionImportRun>> {
    let rows = sqlx::query(IMPORT_RUN_SELECT_BY_RELEASE)
        .bind(release_id.to_string())
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(|row| map_import_run(&row)).collect()
}

pub async fn list_import_file_links_by_release(
    pool: &AnyPool,
    release_id: Uuid,
) -> Result<Vec<AcquisitionImportFileLink>> {
    let rows = sqlx::query(IMPORT_FILE_LINK_SELECT_BY_RELEASE)
        .bind(release_id.to_string())
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|row| map_import_file_link(&row))
        .collect()
}

pub async fn reset_import_runs_for_release(
    pool: &AnyPool,
    release_id: Uuid,
    reason: &str,
    reset_verification: bool,
) -> Result<usize> {
    let link_reason = if reset_verification {
        "Import verification retry requested."
    } else {
        "Import retry requested."
    };
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_import_file_links
         SET state = ?,
             state_reason = ?,
             verification_state = CASE WHEN ? THEN NULL ELSE verification_state END,
             mismatch_class = NULL,
             media_file_id = NULL,
             movie_id = NULL,
             episode_id = NULL,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = ?
           AND state <> ?",
    )
    .bind(AcquisitionImportFileLinkState::Pending.as_str())
    .bind(link_reason)
    .bind(reset_verification)
    .bind(release_id.to_string())
    .bind(AcquisitionImportFileLinkState::Imported.as_str())
    .execute(pool)
    .await
    .context("resetting acquisition import file links")?;

    if reset_verification {
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_file_hashes
             SET hash_status = 'invalidated',
                 hash_invalidated_at = CURRENT_TIMESTAMP,
                 updated_at = CURRENT_TIMESTAMP
             WHERE release_file_id IN (
                SELECT release_file_id
                FROM acquisition_release_files
                WHERE release_id = ?
             )
             OR file_path IN (
                SELECT local_path
                FROM acquisition_import_file_links
                WHERE release_id = ?
                  AND local_path IS NOT NULL
             )",
        )
        .bind(release_id.to_string())
        .bind(release_id.to_string())
        .execute(pool)
        .await
        .context("invalidating anime import hashes")?;
    }

    let result = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_import_runs
         SET state = ?,
             state_reason = ?,
             mismatch_class = NULL,
             retry_count = retry_count + 1,
             completed_at = NULL,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = ?
           AND state <> ?",
    )
    .bind(AcquisitionImportRunState::Pending.as_str())
    .bind(reason)
    .bind(release_id.to_string())
    .bind(AcquisitionImportRunState::Imported.as_str())
    .execute(pool)
    .await
    .context("resetting acquisition import runs")?;
    Ok(result.rows_affected() as usize)
}

pub async fn cancel_import_runs_for_release(
    pool: &AnyPool,
    release_id: Uuid,
    reason: &str,
) -> Result<usize> {
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_import_file_links
         SET state = ?,
             state_reason = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = ?
           AND state <> ?",
    )
    .bind(AcquisitionImportFileLinkState::Skipped.as_str())
    .bind(reason)
    .bind(release_id.to_string())
    .bind(AcquisitionImportFileLinkState::Imported.as_str())
    .execute(pool)
    .await
    .context("cancelling acquisition import file links")?;

    let result = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_import_runs
         SET state = ?,
             state_reason = ?,
             completed_at = CURRENT_TIMESTAMP,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = ?
           AND state NOT IN (?, ?)",
    )
    .bind(AcquisitionImportRunState::Cancelled.as_str())
    .bind(reason)
    .bind(release_id.to_string())
    .bind(AcquisitionImportRunState::Imported.as_str())
    .bind(AcquisitionImportRunState::Cancelled.as_str())
    .execute(pool)
    .await
    .context("cancelling acquisition import runs")?;
    Ok(result.rows_affected() as usize)
}

async fn find_import_file_link(
    pool: &AnyPool,
    data: &NewAcquisitionImportFileLink,
) -> Result<Option<AcquisitionImportFileLink>> {
    if let Some(import_link_id) = data.import_link_id {
        return get_import_file_link(pool, import_link_id).await;
    }
    let row = if let Some(release_file_id) = data.release_file_id {
        sqlx::query(IMPORT_FILE_LINK_SELECT_BY_FILE_TARGET)
            .bind(data.import_run_id.to_string())
            .bind(release_file_id.to_string())
            .bind(data.target_id.map(|value| value.to_string()))
            .fetch_optional(pool)
            .await?
    } else {
        sqlx::query(IMPORT_FILE_LINK_SELECT_BY_TARGET_WITHOUT_FILE)
            .bind(data.import_run_id.to_string())
            .bind(data.target_id.map(|value| value.to_string()))
            .fetch_optional(pool)
            .await?
    };
    row.map(|row| map_import_file_link(&row)).transpose()
}

async fn get_import_file_link(
    pool: &AnyPool,
    import_link_id: Uuid,
) -> Result<Option<AcquisitionImportFileLink>> {
    let row = sqlx::query(IMPORT_FILE_LINK_SELECT_BY_ID)
        .bind(import_link_id.to_string())
        .fetch_optional(pool)
        .await?;
    row.map(|row| map_import_file_link(&row)).transpose()
}

fn validate_import_run_input(data: &NewAcquisitionImportRun) -> Result<()> {
    if data.route_logical_id.trim().is_empty() {
        bail!("import run route_logical_id is required");
    }
    if data.retry_count < 0 {
        bail!("import run retry_count cannot be negative");
    }
    Ok(())
}

fn validate_import_file_link_input(data: &NewAcquisitionImportFileLink) -> Result<()> {
    if data.release_file_id.is_none() && data.target_id.is_none() {
        bail!("import file link requires a release_file_id or target_id");
    }
    Ok(())
}

fn import_run_provenance(
    release: &AcquisitionRelease,
    job: &AcquisitionReleaseJob,
    coverage: &[AcquisitionReleaseCoverage],
    release_files: &[AcquisitionReleaseFile],
) -> JsonValue {
    let coverage_rows = coverage
        .iter()
        .map(|row| {
            json!({
                "coverageId": row.coverage_id,
                "releaseFileId": row.release_file_id,
                "targetId": row.target_id,
                "coverageKind": row.coverage_kind.as_str(),
                "confidence": row.confidence.as_str(),
                "state": row.state.as_str(),
                "verifiedBy": row.verified_by,
                "reason": row.reason,
            })
        })
        .collect::<Vec<_>>();
    let selected_release_files = release_files
        .iter()
        .filter(|file| file.selected == Some(true))
        .map(import_release_file_evidence)
        .collect::<Vec<_>>();
    let manual_mappings = coverage
        .iter()
        .filter(|row| {
            row.coverage_kind == ReleaseCoverageKind::ManualOverride
                || row
                    .verified_by
                    .as_deref()
                    .map(|value| value.starts_with("user:"))
                    .unwrap_or(false)
        })
        .map(|row| {
            json!({
                "coverageId": row.coverage_id,
                "releaseFileId": row.release_file_id,
                "targetId": row.target_id,
                "coverageKind": row.coverage_kind.as_str(),
                "verifiedBy": row.verified_by,
                "reason": row.reason,
            })
        })
        .collect::<Vec<_>>();
    let movie_import_evidence = movie_import_evidence(release, job, coverage, release_files);
    json!({
        "phase": "rr8a",
        "releaseId": release.release_id,
        "releaseJobId": job.release_job_id,
        "subscriptionId": release.subscription_id,
        "mediaType": release.media_type.as_str(),
        "releaseTitle": release.release_title,
        "releaseFingerprint": release.fingerprint,
        "routeLogicalId": job.route_logical_id,
        "providerId": job.provider_id,
        "downloadId": job.download_id,
        "remoteReleaseId": job.remote_release_id,
        "releaseState": release.state.as_str(),
        "jobState": job.state.as_str(),
        "resolverKind": release.resolver_kind.as_str(),
        "resolverVersion": release.resolver_version,
        "selectedCandidate": release.selected_candidate.clone(),
        "coveragePlan": release.coverage_plan,
        "coverage": coverage_rows,
        "selectedReleaseFiles": selected_release_files,
        "movieImportEvidence": movie_import_evidence,
        "manualCoverageMappings": manual_mappings,
        "manualReview": release.coverage_plan.as_ref().and_then(|plan| plan.get("manualReview")),
    })
}

fn import_link_evidence(
    release: &AcquisitionRelease,
    job: &AcquisitionReleaseJob,
    file: Option<&AcquisitionReleaseFile>,
    target_id: Option<Uuid>,
    local_path: Option<&str>,
    coverage: Option<&AcquisitionReleaseCoverage>,
    reason: &str,
) -> JsonValue {
    json!({
        "phase": "rr8a",
        "releaseId": release.release_id,
        "releaseJobId": job.release_job_id,
        "releaseFileId": file.map(|file| file.release_file_id),
        "providerFileId": file.and_then(|file| file.provider_file_id.clone()),
        "file": file.map(import_release_file_evidence),
        "targetId": target_id,
        "localPath": local_path,
        "routeLogicalId": job.route_logical_id,
        "downloadId": job.download_id,
        "remoteReleaseId": job.remote_release_id,
        "coverage": coverage.map(|row| json!({
            "coverageId": row.coverage_id,
            "coverageKind": row.coverage_kind.as_str(),
            "confidence": row.confidence.as_str(),
            "state": row.state.as_str(),
            "verifiedBy": row.verified_by,
            "reason": row.reason,
        })),
        "movieImportEvidence": movie_import_link_evidence(release, job, file, coverage),
        "reason": reason,
    })
}

fn import_release_file_evidence(file: &AcquisitionReleaseFile) -> JsonValue {
    json!({
        "releaseFileId": file.release_file_id,
        "fileIndex": file.file_index,
        "fileId": file.file_id,
        "providerFileId": file.provider_file_id,
        "path": file.path,
        "basename": file.basename,
        "sizeBytes": file.size_bytes,
        "selectable": file.selectable,
        "selected": file.selected,
        "parsed": {
            "title": file.parsed_title,
            "seasonNumber": file.parsed_season_number,
            "episodeNumber": file.parsed_episode_number,
            "episodeEndNumber": file.parsed_episode_end_number,
            "absoluteEpisodeNumber": file.parsed_absolute_episode_number,
            "absoluteEpisodeEndNumber": file.parsed_absolute_episode_end_number,
            "airDate": file.parsed_air_date,
            "quality": file.parsed_quality,
            "language": file.parsed_language,
            "releaseGroup": file.parsed_release_group,
            "confidence": file.parser_confidence.as_str(),
            "reason": file.parser_reason,
        },
    })
}

fn movie_import_evidence(
    release: &AcquisitionRelease,
    job: &AcquisitionReleaseJob,
    coverage: &[AcquisitionReleaseCoverage],
    release_files: &[AcquisitionReleaseFile],
) -> Option<JsonValue> {
    if release.media_type != MediaType::Movie {
        return None;
    }
    let source_plan = movie_source_coverage_plan(release);
    let parsed_release = source_plan
        .as_ref()
        .and_then(|plan| plan.get("parsedRelease"))
        .cloned();
    let graph = source_plan
        .as_ref()
        .and_then(|plan| plan.get("graph"))
        .cloned();
    let reconciliation = source_plan
        .as_ref()
        .and_then(|plan| plan.get("reconciliation"))
        .cloned();
    let selected_coverage_files = coverage
        .iter()
        .filter(|row| row.release_file_id.is_some())
        .filter(|row| movie_import_coverage_kind_allowed(row.coverage_kind))
        .map(|row| {
            let file = row.release_file_id.and_then(|release_file_id| {
                release_files
                    .iter()
                    .find(|file| file.release_file_id == release_file_id)
            });
            json!({
                "coverageId": row.coverage_id,
                "releaseFileId": row.release_file_id,
                "targetId": row.target_id,
                "coverageKind": row.coverage_kind.as_str(),
                "confidence": row.confidence.as_str(),
                "state": row.state.as_str(),
                "verifiedBy": row.verified_by,
                "reason": row.reason,
                "file": file.map(import_release_file_evidence),
            })
        })
        .collect::<Vec<_>>();
    Some(json!({
        "resolver": {
            "kind": release.resolver_kind.as_str(),
            "version": release.resolver_version,
            "confidence": release.confidence.as_str(),
        },
        "route": {
            "routeLogicalId": job.route_logical_id,
            "providerId": job.provider_id,
            "downloadId": job.download_id,
            "remoteReleaseId": job.remote_release_id,
            "releaseSelectedRouteLogicalId": release.selected_route_logical_id,
            "releaseSelectedProviderId": release.selected_provider_id,
            "sourceProviderId": release.source_provider_id,
        },
        "sourceMovieCoveragePlan": source_plan,
        "parsedRelease": parsed_release,
        "graph": graph,
        "reconciliation": reconciliation,
        "fileSelection": movie_file_selection_evidence(release.coverage_plan.as_ref()),
        "selectionPolicy": movie_selection_policy_evidence(release.coverage_plan.as_ref()),
        "runtime": movie_runtime_evidence(release.coverage_plan.as_ref()),
        "selectedCoverageFiles": selected_coverage_files,
    }))
}

fn movie_import_link_evidence(
    release: &AcquisitionRelease,
    job: &AcquisitionReleaseJob,
    file: Option<&AcquisitionReleaseFile>,
    coverage: Option<&AcquisitionReleaseCoverage>,
) -> Option<JsonValue> {
    if release.media_type != MediaType::Movie {
        return None;
    }
    let source_plan = movie_source_coverage_plan(release);
    let parsed_release = source_plan
        .as_ref()
        .and_then(|plan| plan.get("parsedRelease"))
        .cloned();
    let graph = source_plan
        .as_ref()
        .and_then(|plan| plan.get("graph"))
        .cloned();
    let reconciliation = source_plan
        .as_ref()
        .and_then(|plan| plan.get("reconciliation"))
        .cloned();
    Some(json!({
        "resolver": {
            "kind": release.resolver_kind.as_str(),
            "version": release.resolver_version,
        },
        "route": {
            "routeLogicalId": job.route_logical_id,
            "providerId": job.provider_id,
            "downloadId": job.download_id,
            "remoteReleaseId": job.remote_release_id,
        },
        "releaseFileId": file.map(|file| file.release_file_id),
        "targetId": coverage.map(|row| row.target_id),
        "coverageKind": coverage.map(|row| row.coverage_kind.as_str()),
        "sourceMovieCoveragePlan": source_plan,
        "parsedRelease": parsed_release,
        "graph": graph,
        "reconciliation": reconciliation,
        "fileSelection": movie_file_selection_evidence(release.coverage_plan.as_ref()),
        "selectionPolicy": movie_selection_policy_evidence(release.coverage_plan.as_ref()),
        "runtime": movie_runtime_evidence(release.coverage_plan.as_ref()),
    }))
}

fn movie_source_coverage_plan(release: &AcquisitionRelease) -> Option<JsonValue> {
    [
        release
            .selected_candidate
            .as_ref()
            .and_then(|value| value.get("movieCoveragePlan")),
        release
            .coverage_plan
            .as_ref()
            .and_then(|value| value.get("movieCoveragePlan")),
        release
            .coverage_plan
            .as_ref()
            .and_then(|value| value.get("coveragePlan")),
        release
            .coverage_plan
            .as_ref()
            .and_then(|value| value.pointer("/resolverEvidence/parsedRelease/coveragePlan")),
        release.coverage_plan.as_ref().filter(|value| {
            value.get("parsedRelease").is_some()
                || value.get("graph").is_some()
                || value.get("reconciliation").is_some()
        }),
    ]
    .into_iter()
    .flatten()
    .next()
    .cloned()
}

fn movie_file_selection_evidence(plan: Option<&JsonValue>) -> Option<JsonValue> {
    let plan = plan?;
    plan.pointer("/movie/fileSelection")
        .or_else(|| plan.get("fileSelection"))
        .cloned()
}

fn movie_selection_policy_evidence(plan: Option<&JsonValue>) -> Option<JsonValue> {
    let plan = plan?;
    plan.get("selectionPolicy")
        .or_else(|| plan.get("priorityPolicy"))
        .or_else(|| plan.get("manualReview"))
        .cloned()
}

fn movie_runtime_evidence(plan: Option<&JsonValue>) -> Option<JsonValue> {
    let plan = plan?;
    let debrid = plan.get("debridRuntime").cloned();
    let torrent = plan
        .get("torrentRuntime")
        .or_else(|| plan.get("qbittorrentRuntime"))
        .cloned();
    if debrid.is_none() && torrent.is_none() {
        return None;
    }
    Some(json!({
        "debrid": debrid,
        "torrent": torrent,
    }))
}

fn map_release_job(row: &AnyRow) -> Result<AcquisitionReleaseJob> {
    let release_job_id: String = row.try_get("release_job_id")?;
    let release_id: String = row.try_get("release_id")?;
    let provider_id = row_get_opt_string(row, "provider_id")?;
    let state: String = row.try_get("state")?;
    Ok(AcquisitionReleaseJob {
        release_job_id: parse_uuid(&release_job_id, "acquisition_release_jobs.release_job_id")?,
        release_id: parse_uuid(&release_id, "acquisition_release_jobs.release_id")?,
        route_logical_id: row.try_get("route_logical_id")?,
        provider_id: provider_id
            .as_deref()
            .map(|value| parse_uuid(value, "acquisition_release_jobs.provider_id"))
            .transpose()?,
        download_id: row_get_opt_string(row, "download_id")?,
        remote_release_id: row_get_opt_string(row, "remote_release_id")?,
        state: ReleaseJobState::from_str(&state)?,
        state_reason: row_get_opt_string(row, "state_reason")?,
        active: row.try_get::<i64, _>("active")? != 0,
        started_at: row_get_opt_string(row, "started_at")?
            .as_deref()
            .map(|value| parse_datetime(value, "acquisition_release_jobs.started_at"))
            .transpose()?,
        completed_at: row_get_opt_string(row, "completed_at")?
            .as_deref()
            .map(|value| parse_datetime(value, "acquisition_release_jobs.completed_at"))
            .transpose()?,
        created_at: parse_datetime(
            &row.try_get::<String, _>("created_at")?,
            "acquisition_release_jobs.created_at",
        )?,
        updated_at: parse_datetime(
            &row.try_get::<String, _>("updated_at")?,
            "acquisition_release_jobs.updated_at",
        )?,
    })
}

fn map_import_run(row: &AnyRow) -> Result<AcquisitionImportRun> {
    let import_run_id: String = row.try_get("import_run_id")?;
    let release_id: String = row.try_get("release_id")?;
    let release_job_id: String = row.try_get("release_job_id")?;
    let provider_id = row_get_opt_string(row, "provider_id")?;
    let state: String = row.try_get("state")?;
    Ok(AcquisitionImportRun {
        import_run_id: parse_uuid(&import_run_id, "acquisition_import_runs.import_run_id")?,
        release_id: parse_uuid(&release_id, "acquisition_import_runs.release_id")?,
        release_job_id: parse_uuid(&release_job_id, "acquisition_import_runs.release_job_id")?,
        route_logical_id: row.try_get("route_logical_id")?,
        provider_id: provider_id
            .as_deref()
            .map(|value| parse_uuid(value, "acquisition_import_runs.provider_id"))
            .transpose()?,
        download_id: row_get_opt_string(row, "download_id")?,
        remote_release_id: row_get_opt_string(row, "remote_release_id")?,
        state: AcquisitionImportRunState::from_str(&state)?,
        state_reason: row_get_opt_string(row, "state_reason")?,
        mismatch_class: row_get_opt_string(row, "mismatch_class")?,
        retry_count: row.try_get("retry_count")?,
        provenance: parse_json_opt(
            row_get_opt_string(row, "provenance_json")?,
            "acquisition_import_runs.provenance_json",
        )?,
        started_at: row_get_opt_string(row, "started_at")?
            .as_deref()
            .map(|value| parse_datetime(value, "acquisition_import_runs.started_at"))
            .transpose()?,
        completed_at: row_get_opt_string(row, "completed_at")?
            .as_deref()
            .map(|value| parse_datetime(value, "acquisition_import_runs.completed_at"))
            .transpose()?,
        created_at: parse_datetime(
            &row.try_get::<String, _>("created_at")?,
            "acquisition_import_runs.created_at",
        )?,
        updated_at: parse_datetime(
            &row.try_get::<String, _>("updated_at")?,
            "acquisition_import_runs.updated_at",
        )?,
    })
}

fn map_import_file_link(row: &AnyRow) -> Result<AcquisitionImportFileLink> {
    let import_link_id: String = row.try_get("import_link_id")?;
    let import_run_id: String = row.try_get("import_run_id")?;
    let release_id: String = row.try_get("release_id")?;
    let release_file_id = row_get_opt_string(row, "release_file_id")?;
    let target_id = row_get_opt_string(row, "target_id")?;
    let media_file_id = row_get_opt_string(row, "media_file_id")?;
    let movie_id = row_get_opt_string(row, "movie_id")?;
    let episode_id = row_get_opt_string(row, "episode_id")?;
    let state: String = row.try_get("state")?;
    Ok(AcquisitionImportFileLink {
        import_link_id: parse_uuid(
            &import_link_id,
            "acquisition_import_file_links.import_link_id",
        )?,
        import_run_id: parse_uuid(
            &import_run_id,
            "acquisition_import_file_links.import_run_id",
        )?,
        release_id: parse_uuid(&release_id, "acquisition_import_file_links.release_id")?,
        release_file_id: release_file_id
            .as_deref()
            .map(|value| parse_uuid(value, "acquisition_import_file_links.release_file_id"))
            .transpose()?,
        target_id: target_id
            .as_deref()
            .map(|value| parse_uuid(value, "acquisition_import_file_links.target_id"))
            .transpose()?,
        local_path: row_get_opt_string(row, "local_path")?,
        media_file_id: media_file_id
            .as_deref()
            .map(|value| parse_uuid(value, "acquisition_import_file_links.media_file_id"))
            .transpose()?,
        movie_id: movie_id
            .as_deref()
            .map(|value| parse_uuid(value, "acquisition_import_file_links.movie_id"))
            .transpose()?,
        episode_id: episode_id
            .as_deref()
            .map(|value| parse_uuid(value, "acquisition_import_file_links.episode_id"))
            .transpose()?,
        state: AcquisitionImportFileLinkState::from_str(&state)?,
        state_reason: row_get_opt_string(row, "state_reason")?,
        verification_state: row_get_opt_string(row, "verification_state")?,
        mismatch_class: row_get_opt_string(row, "mismatch_class")?,
        evidence: parse_json_opt(
            row_get_opt_string(row, "evidence_json")?,
            "acquisition_import_file_links.evidence_json",
        )?,
        created_at: parse_datetime(
            &row.try_get::<String, _>("created_at")?,
            "acquisition_import_file_links.created_at",
        )?,
        updated_at: parse_datetime(
            &row.try_get::<String, _>("updated_at")?,
            "acquisition_import_file_links.updated_at",
        )?,
    })
}

fn parse_uuid(value: &str, field: &str) -> Result<Uuid> {
    Uuid::parse_str(value).with_context(|| format!("invalid uuid in {field}"))
}

fn row_get_opt_string(row: &AnyRow, column: &str) -> Result<Option<String>> {
    row.try_get::<Option<String>, _>(column)
        .map(|value| value.and_then(|value| non_empty_string(&value)))
        .or_else(|_| {
            row.try_get::<String, _>(column)
                .map(|value| non_empty_string(&value))
        })
        .with_context(|| format!("reading optional column {column}"))
}

fn non_empty_string(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn parse_datetime(value: &str, field: &str) -> Result<DateTime<Utc>> {
    if let Ok(datetime) = DateTime::parse_from_rfc3339(value) {
        return Ok(datetime.with_timezone(&Utc));
    }
    let naive = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S"))
        .with_context(|| format!("invalid datetime in {field}"))?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
}

fn db_datetime_string(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%d %H:%M:%S%.f").to_string()
}

fn db_datetime_string_now() -> String {
    db_datetime_string(Utc::now())
}

fn json_to_string(value: Option<&JsonValue>) -> Result<Option<String>> {
    value
        .map(serde_json::to_string)
        .transpose()
        .context("serializing acquisition import json")
}

fn parse_json_opt(value: Option<String>, field: &str) -> Result<Option<JsonValue>> {
    value
        .map(|value| {
            serde_json::from_str(&value).with_context(|| format!("invalid json in {field}"))
        })
        .transpose()
}

macro_rules! import_run_columns {
    () => {
        "import_run_id,
release_id,
release_job_id,
route_logical_id,
COALESCE(CAST(provider_id AS TEXT), '') AS provider_id,
COALESCE(CAST(download_id AS TEXT), '') AS download_id,
COALESCE(CAST(remote_release_id AS TEXT), '') AS remote_release_id,
state,
COALESCE(CAST(state_reason AS TEXT), '') AS state_reason,
COALESCE(CAST(mismatch_class AS TEXT), '') AS mismatch_class,
retry_count,
COALESCE(CAST(provenance_json AS TEXT), '') AS provenance_json,
COALESCE(CAST(started_at AS TEXT), '') AS started_at,
COALESCE(CAST(completed_at AS TEXT), '') AS completed_at,
CAST(created_at AS TEXT) AS created_at,
CAST(updated_at AS TEXT) AS updated_at"
    };
}

const IMPORT_RUN_SELECT_BY_ID: &str = concat!(
    "SELECT ",
    import_run_columns!(),
    " FROM acquisition_import_runs WHERE import_run_id = ? LIMIT 1"
);

const IMPORT_RUN_SELECT_BY_RELEASE_JOB: &str = concat!(
    "SELECT ",
    import_run_columns!(),
    " FROM acquisition_import_runs WHERE release_job_id = ? LIMIT 1"
);

const IMPORT_RUN_SELECT_BY_RELEASE: &str = concat!(
    "SELECT ",
    import_run_columns!(),
    " FROM acquisition_import_runs WHERE release_id = ? ORDER BY created_at, import_run_id"
);

macro_rules! import_file_link_columns {
    () => {
        "import_link_id,
import_run_id,
release_id,
COALESCE(CAST(release_file_id AS TEXT), '') AS release_file_id,
COALESCE(CAST(target_id AS TEXT), '') AS target_id,
COALESCE(CAST(local_path AS TEXT), '') AS local_path,
COALESCE(CAST(media_file_id AS TEXT), '') AS media_file_id,
COALESCE(CAST(movie_id AS TEXT), '') AS movie_id,
COALESCE(CAST(episode_id AS TEXT), '') AS episode_id,
state,
COALESCE(CAST(state_reason AS TEXT), '') AS state_reason,
COALESCE(CAST(verification_state AS TEXT), '') AS verification_state,
COALESCE(CAST(mismatch_class AS TEXT), '') AS mismatch_class,
COALESCE(CAST(evidence_json AS TEXT), '') AS evidence_json,
CAST(created_at AS TEXT) AS created_at,
CAST(updated_at AS TEXT) AS updated_at"
    };
}

const IMPORT_FILE_LINK_SELECT_BY_ID: &str = concat!(
    "SELECT ",
    import_file_link_columns!(),
    " FROM acquisition_import_file_links WHERE import_link_id = ? LIMIT 1"
);

const IMPORT_FILE_LINK_SELECT_BY_RUN: &str = concat!(
    "SELECT ",
    import_file_link_columns!(),
    " FROM acquisition_import_file_links WHERE import_run_id = ? ORDER BY target_id, release_file_id"
);

const IMPORT_FILE_LINK_SELECT_BY_RELEASE: &str = concat!(
    "SELECT ",
    import_file_link_columns!(),
    " FROM acquisition_import_file_links WHERE release_id = ? ORDER BY target_id, release_file_id"
);

const IMPORT_FILE_LINK_SELECT_BY_FILE_TARGET: &str = concat!(
    "SELECT ",
    import_file_link_columns!(),
    " FROM acquisition_import_file_links WHERE import_run_id = ? AND release_file_id = ? AND target_id = ? LIMIT 1"
);

const IMPORT_FILE_LINK_SELECT_BY_TARGET_WITHOUT_FILE: &str = concat!(
    "SELECT ",
    import_file_link_columns!(),
    " FROM acquisition_import_file_links WHERE import_run_id = ? AND release_file_id IS NULL AND target_id = ? LIMIT 1"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        acquisition::{
            audit::count_acquisition_audit_events,
            release_resolution::{
                anidb::{
                    AniDbChannel, AniDbRateLimiterConfig, build_lookup_key,
                    reserve_anidb_channel_request,
                },
                hashing::hash_local_file,
                models::{
                    AcquisitionReleaseState, AniDbFileLookupStatus, AnimeMatchOutcome,
                    NewAcquisitionAniDbFileCache, NewAcquisitionRelease,
                    NewAcquisitionReleaseCoverage, NewAcquisitionReleaseFile,
                    NewAcquisitionReleaseJob, ReleaseConfidence, ReleaseCoverageKind, ReleaseKind,
                    ReleaseResolverKind,
                },
                store::{
                    list_anime_identity_mismatches_by_release,
                    list_anime_match_attempts_by_release, list_release_coverage,
                    upsert_anidb_file_cache, upsert_release, upsert_release_coverage,
                    upsert_release_file, upsert_release_job,
                },
            },
            subscriptions::{
                AcquisitionCompletionPolicy, AcquisitionMetadataPolicy, AcquisitionMonitorPolicy,
                AcquisitionRequestMode, AcquisitionRequestScope, AcquisitionRoutePolicy,
                AcquisitionTargetState, NewAcquisitionSubscription, NewAcquisitionTarget,
                create_subscription, get_target, upsert_subscription_targets,
            },
        },
        config::DatabaseConfig,
        db::{Database, models::MediaType},
        download_broker::{
            DEBRID_DEFAULT_LOGICAL_ID, DEFAULT_ROUTE_OWNER_ID, TORRENT_DEFAULT_LOGICAL_ID,
        },
        extensions::ExternalIds,
    };
    use tempfile::tempdir;

    struct ImportFixture {
        database: Database,
        subscription_id: Uuid,
        target_id: Uuid,
        release_id: Uuid,
        job_id: Uuid,
        local_path: String,
    }

    async fn setup_db() -> Result<Database> {
        let database = Database::connect(&DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        })
        .await?;
        database.run_migrations().await?;
        Ok(database)
    }

    async fn setup_completed_release(
        route: &str,
        local_path: Option<String>,
        coverage_state: ReleaseCoverageState,
        file_selected: Option<bool>,
    ) -> Result<ImportFixture> {
        let database = setup_db().await?;
        if let Some(path) = local_path.as_deref() {
            if let Some(parent) = Path::new(path).parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            tokio::fs::write(path, b"video").await?;
        }
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Series,
                title: "Show".to_string(),
                year: Some(2024),
                external_ids: None,
                idempotency_key: None,
                request_mode: None,
                request_scope: None,
                scope: None,
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: AcquisitionMonitorPolicy::AllMissing,
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: None,
                release_delay_seconds: Some(0),
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
            },
        )
        .await?;
        let targets = upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![NewAcquisitionTarget {
                target_key: Some("S01E01".to_string()),
                media_type: Some(MediaType::Series),
                title: Some("Show".to_string()),
                season_number: Some(1),
                episode_number: Some(1),
                absolute_episode_number: None,
                air_date: None,
                air_time: None,
                metadata: None,
                state: Some(AcquisitionTargetState::Submitted),
                next_search_after: None,
            }],
        )
        .await?;
        let target = targets
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("missing test target"))?;
        let release = upsert_release(
            &database.pool,
            NewAcquisitionRelease {
                release_id: None,
                subscription_id: Some(subscription.subscription_id),
                source_provider_id: None,
                source_extension_id: "test.source".to_string(),
                owner_id: DEFAULT_ROUTE_OWNER_ID.to_string(),
                media_type: MediaType::Series,
                title: "Show".to_string(),
                release_title: "Show.S01E01.1080p.WEB-DL-GROUP".to_string(),
                source: "magnet:?xt=urn:btih:test".to_string(),
                source_kind: "magnet".to_string(),
                info_hash: Some("test".to_string()),
                fingerprint: format!("fingerprint-{route}"),
                release_kind: ReleaseKind::Single,
                resolver_kind: ReleaseResolverKind::TvSonarrStyle,
                resolver_version: "test".to_string(),
                confidence: ReleaseConfidence::High,
                score: None,
                selected_route_logical_id: Some(route.to_string()),
                selected_provider_id: None,
                download_id: Some("download-1".to_string()),
                remote_release_id: Some("remote-1".to_string()),
                state: AcquisitionReleaseState::Completed,
                state_reason: Some("completed".to_string()),
                selected_candidate: None,
                coverage_plan: Some(json!({ "test": true })),
            },
        )
        .await?;
        let file = upsert_release_file(
            &database.pool,
            NewAcquisitionReleaseFile {
                release_file_id: None,
                release_id: release.release_id,
                file_index: Some(0),
                file_id: Some("1".to_string()),
                provider_file_id: Some("1".to_string()),
                path: "Show.S01E01.mkv".to_string(),
                basename: Some("Show.S01E01.mkv".to_string()),
                size_bytes: Some(5),
                selectable: true,
                selected: file_selected,
                parsed_title: Some("show".to_string()),
                parsed_season_number: Some(1),
                parsed_episode_number: Some(1),
                parsed_episode_end_number: None,
                parsed_absolute_episode_number: None,
                parsed_absolute_episode_end_number: None,
                parsed_air_date: None,
                parsed_quality: Some("1080p".to_string()),
                parsed_language: None,
                parsed_release_group: Some("GROUP".to_string()),
                parser_confidence: ReleaseConfidence::High,
                parser_reason: None,
                raw: None,
                provider_metadata: local_path.as_ref().map(|path| json!({ "localPath": path })),
            },
        )
        .await?;
        upsert_release_coverage(
            &database.pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id: Some(file.release_file_id),
                target_id: target.target_id,
                coverage_kind: ReleaseCoverageKind::SingleEpisode,
                confidence: ReleaseConfidence::High,
                score: Some(1.0),
                reason: Some("test coverage".to_string()),
                state: coverage_state,
                verified_by: Some("test".to_string()),
            },
        )
        .await?;
        let job = upsert_release_job(
            &database.pool,
            NewAcquisitionReleaseJob {
                release_job_id: None,
                release_id: release.release_id,
                route_logical_id: route.to_string(),
                provider_id: None,
                download_id: Some("download-1".to_string()),
                remote_release_id: Some("remote-1".to_string()),
                state: ReleaseJobState::Completed,
                state_reason: Some("completed".to_string()),
                active: false,
                started_at: Some(Utc::now()),
                completed_at: Some(Utc::now()),
            },
        )
        .await?;

        Ok(ImportFixture {
            database,
            subscription_id: subscription.subscription_id,
            target_id: target.target_id,
            release_id: release.release_id,
            job_id: job.release_job_id,
            local_path: local_path.unwrap_or_default(),
        })
    }

    struct TestReleaseFile {
        local_path: String,
        basename: String,
        file_index: i64,
        selected: Option<bool>,
        parsed_season: Option<i32>,
        parsed_episode: Option<i32>,
        coverage: Vec<(Uuid, ReleaseCoverageKind, ReleaseCoverageState)>,
    }

    async fn insert_completed_release_with_files(
        database: &Database,
        subscription_id: Uuid,
        media_type: MediaType,
        release_title: &str,
        release_kind: ReleaseKind,
        resolver_kind: ReleaseResolverKind,
        route: &str,
        fingerprint: &str,
        files: Vec<TestReleaseFile>,
    ) -> Result<(Uuid, Uuid)> {
        insert_completed_release_with_files_and_plan(
            database,
            subscription_id,
            media_type,
            release_title,
            release_kind,
            resolver_kind,
            route,
            fingerprint,
            files,
            Some(json!({ "fingerprint": fingerprint })),
        )
        .await
    }

    async fn insert_completed_release_with_files_and_plan(
        database: &Database,
        subscription_id: Uuid,
        media_type: MediaType,
        release_title: &str,
        release_kind: ReleaseKind,
        resolver_kind: ReleaseResolverKind,
        route: &str,
        fingerprint: &str,
        files: Vec<TestReleaseFile>,
        coverage_plan: Option<JsonValue>,
    ) -> Result<(Uuid, Uuid)> {
        let release = upsert_release(
            &database.pool,
            NewAcquisitionRelease {
                release_id: None,
                subscription_id: Some(subscription_id),
                source_provider_id: None,
                source_extension_id: "test.source".to_string(),
                owner_id: DEFAULT_ROUTE_OWNER_ID.to_string(),
                media_type,
                title: release_title.to_string(),
                release_title: release_title.to_string(),
                source: format!("magnet:?xt=urn:btih:{fingerprint}"),
                source_kind: "magnet".to_string(),
                info_hash: Some(fingerprint.to_string()),
                fingerprint: fingerprint.to_string(),
                release_kind,
                resolver_kind,
                resolver_version: "test".to_string(),
                confidence: ReleaseConfidence::High,
                score: Some(100.0),
                selected_route_logical_id: Some(route.to_string()),
                selected_provider_id: None,
                download_id: Some(format!("download-{fingerprint}")),
                remote_release_id: Some(format!("remote-{fingerprint}")),
                state: AcquisitionReleaseState::Completed,
                state_reason: Some("completed".to_string()),
                selected_candidate: Some(json!({ "fingerprint": fingerprint })),
                coverage_plan,
            },
        )
        .await?;

        for file in files {
            let release_file = upsert_release_file(
                &database.pool,
                NewAcquisitionReleaseFile {
                    release_file_id: None,
                    release_id: release.release_id,
                    file_index: Some(file.file_index),
                    file_id: Some(file.file_index.to_string()),
                    provider_file_id: Some(file.file_index.to_string()),
                    path: file.basename.clone(),
                    basename: Some(file.basename),
                    size_bytes: Some(5),
                    selectable: true,
                    selected: file.selected,
                    parsed_title: Some(release_title.to_string()),
                    parsed_season_number: file.parsed_season,
                    parsed_episode_number: file.parsed_episode,
                    parsed_episode_end_number: None,
                    parsed_absolute_episode_number: None,
                    parsed_absolute_episode_end_number: None,
                    parsed_air_date: None,
                    parsed_quality: Some("1080p".to_string()),
                    parsed_language: None,
                    parsed_release_group: Some("GROUP".to_string()),
                    parser_confidence: ReleaseConfidence::High,
                    parser_reason: None,
                    raw: None,
                    provider_metadata: Some(json!({ "localPath": file.local_path })),
                },
            )
            .await?;
            for (target_id, coverage_kind, state) in file.coverage {
                upsert_release_coverage(
                    &database.pool,
                    NewAcquisitionReleaseCoverage {
                        coverage_id: None,
                        release_id: release.release_id,
                        release_file_id: Some(release_file.release_file_id),
                        target_id,
                        coverage_kind,
                        confidence: ReleaseConfidence::High,
                        score: Some(1.0),
                        reason: Some("test coverage".to_string()),
                        state,
                        verified_by: Some("test".to_string()),
                    },
                )
                .await?;
            }
        }

        let job = upsert_release_job(
            &database.pool,
            NewAcquisitionReleaseJob {
                release_job_id: None,
                release_id: release.release_id,
                route_logical_id: route.to_string(),
                provider_id: None,
                download_id: Some(format!("download-{fingerprint}")),
                remote_release_id: Some(format!("remote-{fingerprint}")),
                state: ReleaseJobState::Completed,
                state_reason: Some("completed".to_string()),
                active: false,
                started_at: Some(Utc::now()),
                completed_at: Some(Utc::now()),
            },
        )
        .await?;

        Ok((release.release_id, job.release_job_id))
    }

    async fn create_anime_subscription_with_targets(
        database: &Database,
        title: &str,
        episode_ids: &[i64],
    ) -> Result<(AcquisitionSubscription, Vec<AcquisitionTarget>)> {
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Anime,
                title: title.to_string(),
                year: Some(2024),
                external_ids: Some(ExternalIds {
                    anidb: Some("200".to_string()),
                    anilist: Some("100".to_string()),
                    ..ExternalIds::default()
                }),
                idempotency_key: None,
                request_mode: None,
                request_scope: None,
                scope: None,
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: AcquisitionMonitorPolicy::SelectedTargets,
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: None,
                release_delay_seconds: Some(0),
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
            },
        )
        .await?;
        let targets = upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            episode_ids
                .iter()
                .enumerate()
                .map(|(index, anidb_episode_id)| {
                    let episode = i32::try_from(index + 1).expect("test episode fits in i32");
                    NewAcquisitionTarget {
                        target_key: Some(format!("S01E{episode:02}")),
                        media_type: Some(MediaType::Anime),
                        title: Some(format!("Episode {episode}")),
                        season_number: Some(1),
                        episode_number: Some(episode),
                        absolute_episode_number: Some(episode),
                        air_date: None,
                        air_time: None,
                        metadata: Some(json!({
                            "anidbAnimeId": 200,
                            "anidbEpisodeId": anidb_episode_id,
                            "targetCanonicalKey": format!("anidb_episode:{anidb_episode_id}"),
                        })),
                        state: Some(AcquisitionTargetState::Submitted),
                        next_search_after: None,
                    }
                })
                .collect(),
        )
        .await?;
        Ok((subscription, targets))
    }

    async fn create_movie_subscription_with_target(
        database: &Database,
        title: &str,
        year: i32,
    ) -> Result<(Uuid, Uuid)> {
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Movie,
                title: title.to_string(),
                year: Some(year),
                external_ids: None,
                idempotency_key: None,
                request_mode: None,
                request_scope: None,
                scope: None,
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: AcquisitionMonitorPolicy::SelectedTargets,
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: None,
                release_delay_seconds: Some(0),
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
            },
        )
        .await?;
        let targets = upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![NewAcquisitionTarget {
                target_key: Some("movie".to_string()),
                media_type: Some(MediaType::Movie),
                title: Some(title.to_string()),
                season_number: None,
                episode_number: None,
                absolute_episode_number: None,
                air_date: None,
                air_time: None,
                metadata: None,
                state: Some(AcquisitionTargetState::Submitted),
                next_search_after: None,
            }],
        )
        .await?;
        let target = targets
            .first()
            .ok_or_else(|| anyhow!("missing movie test target"))?;
        Ok((subscription.subscription_id, target.target_id))
    }

    async fn seed_anidb_hit_for_file(
        database: &Database,
        path: &Path,
        anidb_episode_ids: Vec<i64>,
    ) -> Result<String> {
        let digest = hash_local_file(path.to_path_buf(), 64 * 1024).await?;
        let lookup_key = build_lookup_key(&digest.ed2k, digest.size_bytes);
        upsert_anidb_file_cache(
            &database.pool,
            NewAcquisitionAniDbFileCache {
                lookup_key: lookup_key.clone(),
                ed2k: digest.ed2k,
                size_bytes: digest.size_bytes,
                lookup_status: AniDbFileLookupStatus::Hit,
                anidb_file_id: Some(100),
                anidb_anime_id: Some(200),
                anidb_episode_ids: json!(anidb_episode_ids),
                anidb_group_id: Some(300),
                anidb_group_name: Some("Group".to_string()),
                anidb_group_short_name: Some("GRP".to_string()),
                anidb_version: Some(1),
                anidb_source: Some("Web".to_string()),
                anidb_quality: Some("1080p".to_string()),
                anidb_audio_languages: json!(["japanese"]),
                anidb_subtitle_languages: json!(["english"]),
                anidb_state_flags: json!(["crc_match"]),
                anidb_original_filename: path
                    .file_name()
                    .map(|value| value.to_string_lossy().to_string()),
                released_at: Some(Utc::now()),
                raw_response: Some("test cache".to_string()),
                positive_cached_at: Some(Utc::now()),
                negative_cached_until: None,
                last_lookup_attempt_at: Some(Utc::now()),
            },
        )
        .await?;
        Ok(lookup_key)
    }

    #[tokio::test]
    async fn rr8a_completed_debrid_job_creates_one_import_run() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("Show.S01E01.mkv");
        let fixture = setup_completed_release(
            DEBRID_DEFAULT_LOGICAL_ID,
            Some(path.to_string_lossy().to_string()),
            ReleaseCoverageState::Submitted,
            Some(true),
        )
        .await?;

        let stats = run_acquisition_import_iteration(&fixture.database.pool, 10).await?;
        assert_eq!(stats.candidates, 1);
        assert_eq!(stats.runs_created, 1);
        assert_eq!(stats.links_upserted, 1);
        assert_eq!(stats.runs_imported, 1);
        assert_eq!(stats.links_imported, 1);
        assert_eq!(stats.blocked_runs, 0);

        let run = get_import_run_by_release_job(&fixture.database.pool, fixture.job_id)
            .await?
            .expect("import run");
        assert_eq!(run.state, AcquisitionImportRunState::Imported);
        assert_eq!(run.release_id, fixture.release_id);
        assert_eq!(run.route_logical_id, DEBRID_DEFAULT_LOGICAL_ID);
        let links = list_import_file_links(&fixture.database.pool, run.import_run_id).await?;
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].state, AcquisitionImportFileLinkState::Imported);
        assert_eq!(
            links[0].local_path.as_deref(),
            Some(fixture.local_path.as_str())
        );
        assert!(links[0].media_file_id.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn acquisition_import_persists_media_ownership() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("Show.S01E01.mkv");
        let fixture = setup_completed_release(
            DEBRID_DEFAULT_LOGICAL_ID,
            Some(path.to_string_lossy().to_string()),
            ReleaseCoverageState::Submitted,
            Some(true),
        )
        .await?;

        let stats = run_acquisition_import_iteration(&fixture.database.pool, 10).await?;
        assert_eq!(stats.runs_imported, 1);

        let row = sqlx::query(
            "SELECT owner_type, owner_label, acquisition_subscription_id, release_capability, release_policy
             FROM media_ownerships
             WHERE active = 1
             LIMIT 1",
        )
        .fetch_one(&fixture.database.pool)
        .await?;
        let owner_type: String = row.try_get("owner_type")?;
        let owner_label: Option<String> = row.try_get("owner_label").ok();
        let subscription_id: Option<String> = row.try_get("acquisition_subscription_id").ok();
        let release_capability: String = row.try_get("release_capability")?;
        let release_policy: String = row.try_get("release_policy")?;
        assert_eq!(owner_type, "acquisition");
        assert_eq!(owner_label.as_deref(), Some("Elixir acquisition"));
        let expected_subscription_id = fixture.subscription_id.to_string();
        assert_eq!(
            subscription_id.as_deref(),
            Some(expected_subscription_id.as_str())
        );
        assert_eq!(release_capability, "acquisition.stop_monitoring");
        assert_eq!(release_policy, "supported");
        Ok(())
    }

    #[tokio::test]
    async fn rr8a_completed_qbittorrent_job_creates_one_import_run() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("Show.S01E01.mkv");
        let fixture = setup_completed_release(
            TORRENT_DEFAULT_LOGICAL_ID,
            Some(path.to_string_lossy().to_string()),
            ReleaseCoverageState::Selected,
            Some(true),
        )
        .await?;

        let stats = run_acquisition_import_iteration(&fixture.database.pool, 10).await?;
        assert_eq!(stats.runs_created, 1);
        assert_eq!(stats.runs_imported, 1);
        let run = get_import_run_by_release_job(&fixture.database.pool, fixture.job_id)
            .await?
            .expect("import run");
        assert_eq!(run.state, AcquisitionImportRunState::Imported);
        assert_eq!(run.route_logical_id, TORRENT_DEFAULT_LOGICAL_ID);
        Ok(())
    }

    #[tokio::test]
    async fn rr8a_repeated_import_pass_is_idempotent() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("Show.S01E01.mkv");
        let fixture = setup_completed_release(
            DEBRID_DEFAULT_LOGICAL_ID,
            Some(path.to_string_lossy().to_string()),
            ReleaseCoverageState::Submitted,
            Some(true),
        )
        .await?;

        run_acquisition_import_iteration(&fixture.database.pool, 10).await?;
        let second = run_acquisition_import_iteration(&fixture.database.pool, 10).await?;
        assert_eq!(second.candidates, 0);
        let run = get_import_run_by_release_job(&fixture.database.pool, fixture.job_id)
            .await?
            .expect("import run");
        let links = list_import_file_links(&fixture.database.pool, run.import_run_id).await?;
        assert_eq!(links.len(), 1);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM acquisition_import_runs")
            .fetch_one(&fixture.database.pool)
            .await?;
        assert_eq!(count, 1);
        let media_file_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_files")
            .fetch_one(&fixture.database.pool)
            .await?;
        assert_eq!(media_file_count, 1);
        Ok(())
    }

    #[tokio::test]
    async fn import_preparation_ignores_placeholder_coverage_when_concrete_mapping_exists()
    -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("Show.S01E01.mkv");
        let fixture = setup_completed_release(
            DEBRID_DEFAULT_LOGICAL_ID,
            Some(path.to_string_lossy().to_string()),
            ReleaseCoverageState::Submitted,
            Some(true),
        )
        .await?;
        upsert_release_coverage(
            &fixture.database.pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: fixture.release_id,
                release_file_id: None,
                target_id: fixture.target_id,
                coverage_kind: ReleaseCoverageKind::ManualOverride,
                confidence: ReleaseConfidence::ReviewRequired,
                score: None,
                reason: Some("stale placeholder row".to_string()),
                state: ReleaseCoverageState::Submitted,
                verified_by: Some("test".to_string()),
            },
        )
        .await?;

        let stats = run_acquisition_import_iteration(&fixture.database.pool, 10).await?;
        assert_eq!(stats.runs_imported, 1);
        assert_eq!(stats.blocked_runs, 0);
        let run = get_import_run_by_release_job(&fixture.database.pool, fixture.job_id)
            .await?
            .expect("import run");
        assert_eq!(run.state, AcquisitionImportRunState::Imported);
        let links = list_import_file_links(&fixture.database.pool, run.import_run_id).await?;
        assert_eq!(links.len(), 1);
        assert!(links[0].release_file_id.is_some());
        assert_eq!(links[0].state, AcquisitionImportFileLinkState::Imported);
        Ok(())
    }

    #[tokio::test]
    async fn import_coordinator_skips_terminal_import_runs_without_starving_new_jobs() -> Result<()>
    {
        let database = setup_db().await?;
        let dir = tempdir()?;
        let blocked_path = dir.path().join("Show.S01E01.Blocked.mkv");
        let ready_path = dir.path().join("Show.S01E01.Ready.mkv");
        tokio::fs::write(&blocked_path, b"blocked").await?;
        tokio::fs::write(&ready_path, b"ready").await?;

        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Series,
                title: "Show".to_string(),
                year: Some(2024),
                external_ids: None,
                idempotency_key: None,
                request_mode: None,
                request_scope: None,
                scope: None,
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: AcquisitionMonitorPolicy::SelectedTargets,
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: None,
                release_delay_seconds: Some(0),
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
            },
        )
        .await?;
        let targets = upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![NewAcquisitionTarget {
                target_key: Some("S01E01".to_string()),
                media_type: Some(MediaType::Series),
                title: Some("Episode 1".to_string()),
                season_number: Some(1),
                episode_number: Some(1),
                absolute_episode_number: None,
                air_date: None,
                air_time: None,
                metadata: None,
                state: Some(AcquisitionTargetState::Submitted),
                next_search_after: None,
            }],
        )
        .await?;
        let target_id = targets
            .first()
            .map(|target| target.target_id)
            .ok_or_else(|| anyhow!("missing test target"))?;

        let (blocked_release_id, blocked_job_id) = insert_completed_release_with_files(
            &database,
            subscription.subscription_id,
            MediaType::Series,
            "Show",
            ReleaseKind::Single,
            ReleaseResolverKind::TvSonarrStyle,
            DEBRID_DEFAULT_LOGICAL_ID,
            "blocked",
            vec![TestReleaseFile {
                local_path: blocked_path.to_string_lossy().to_string(),
                basename: "Show.S01E01.Blocked.mkv".to_string(),
                file_index: 0,
                selected: Some(true),
                parsed_season: Some(1),
                parsed_episode: Some(1),
                coverage: vec![(
                    target_id,
                    ReleaseCoverageKind::SingleEpisode,
                    ReleaseCoverageState::Submitted,
                )],
            }],
        )
        .await?;
        create_or_get_import_run(
            &database.pool,
            NewAcquisitionImportRun {
                import_run_id: None,
                release_id: blocked_release_id,
                release_job_id: blocked_job_id,
                route_logical_id: DEBRID_DEFAULT_LOGICAL_ID.to_string(),
                provider_id: None,
                download_id: Some("download-blocked".to_string()),
                remote_release_id: Some("remote-blocked".to_string()),
                state: AcquisitionImportRunState::Blocked,
                state_reason: Some("Blocked before this coordinator pass.".to_string()),
                mismatch_class: Some("test_blocked".to_string()),
                retry_count: 0,
                provenance: None,
                started_at: None,
                completed_at: Some(Utc::now()),
            },
        )
        .await?;

        let (_ready_release_id, ready_job_id) = insert_completed_release_with_files(
            &database,
            subscription.subscription_id,
            MediaType::Series,
            "Show",
            ReleaseKind::Single,
            ReleaseResolverKind::TvSonarrStyle,
            DEBRID_DEFAULT_LOGICAL_ID,
            "ready",
            vec![TestReleaseFile {
                local_path: ready_path.to_string_lossy().to_string(),
                basename: "Show.S01E01.Ready.mkv".to_string(),
                file_index: 0,
                selected: Some(true),
                parsed_season: Some(1),
                parsed_episode: Some(1),
                coverage: vec![(
                    target_id,
                    ReleaseCoverageKind::SingleEpisode,
                    ReleaseCoverageState::Submitted,
                )],
            }],
        )
        .await?;

        let stats = run_acquisition_import_iteration(&database.pool, 1).await?;
        assert_eq!(stats.candidates, 1);
        assert_eq!(stats.runs_created, 1);
        assert_eq!(stats.runs_imported, 1);
        assert_eq!(stats.blocked_runs, 0);

        let blocked_run = get_import_run_by_release_job(&database.pool, blocked_job_id)
            .await?
            .expect("blocked import run");
        assert_eq!(blocked_run.state, AcquisitionImportRunState::Blocked);
        let ready_run = get_import_run_by_release_job(&database.pool, ready_job_id)
            .await?
            .expect("ready import run");
        assert_eq!(ready_run.state, AcquisitionImportRunState::Imported);
        Ok(())
    }

    #[tokio::test]
    async fn rr8b_movie_import_links_one_target_and_media_file() -> Result<()> {
        let database = setup_db().await?;
        let dir = tempdir()?;
        let path = dir.path().join("Movie.2024.mkv");
        tokio::fs::write(&path, b"movie").await?;
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Movie,
                title: "Movie".to_string(),
                year: Some(2024),
                external_ids: None,
                idempotency_key: None,
                request_mode: None,
                request_scope: None,
                scope: None,
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: AcquisitionMonitorPolicy::SelectedTargets,
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: None,
                release_delay_seconds: Some(0),
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
            },
        )
        .await?;
        let targets = upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![NewAcquisitionTarget {
                target_key: Some("movie".to_string()),
                media_type: Some(MediaType::Movie),
                title: Some("Movie".to_string()),
                season_number: None,
                episode_number: None,
                absolute_episode_number: None,
                air_date: None,
                air_time: None,
                metadata: None,
                state: Some(AcquisitionTargetState::Submitted),
                next_search_after: None,
            }],
        )
        .await?;
        let target = targets[0].clone();
        let (_release_id, job_id) = insert_completed_release_with_files(
            &database,
            subscription.subscription_id,
            MediaType::Movie,
            "Movie.2024.1080p.WEB-DL-GROUP",
            ReleaseKind::Single,
            ReleaseResolverKind::MovieRadarrStyle,
            DEBRID_DEFAULT_LOGICAL_ID,
            "movie-1",
            vec![TestReleaseFile {
                local_path: path.to_string_lossy().to_string(),
                basename: "Movie.2024.mkv".to_string(),
                file_index: 0,
                selected: Some(true),
                parsed_season: None,
                parsed_episode: None,
                coverage: vec![(
                    target.target_id,
                    ReleaseCoverageKind::Movie,
                    ReleaseCoverageState::Submitted,
                )],
            }],
        )
        .await?;

        let stats = run_acquisition_import_iteration(&database.pool, 10).await?;
        assert_eq!(stats.runs_imported, 1);
        assert_eq!(stats.links_imported, 1);
        let run = get_import_run_by_release_job(&database.pool, job_id)
            .await?
            .expect("import run");
        let links = list_import_file_links(&database.pool, run.import_run_id).await?;
        assert_eq!(links.len(), 1);
        assert!(links[0].media_file_id.is_some());
        assert!(links[0].movie_id.is_some());
        assert!(links[0].episode_id.is_none());
        let movie_files: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM movie_files")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(movie_files, 1);
        let imported_target = get_target(&database.pool, target.target_id)
            .await?
            .expect("target");
        assert_eq!(imported_target.state, AcquisitionTargetState::Imported);
        assert_eq!(imported_target.import_event_id, Some(run.import_run_id));
        let subscription_status: String = sqlx::query_scalar(
            "SELECT status FROM acquisition_subscriptions WHERE subscription_id = ?",
        )
        .bind(subscription.subscription_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(subscription_status, "completed");
        Ok(())
    }

    #[tokio::test]
    async fn rrm6_movie_import_uses_selected_movie_coverage_file_only() -> Result<()> {
        let database = setup_db().await?;
        let dir = tempdir()?;
        let main_path = dir.path().join("Movie.2024.1080p.mkv");
        let extra_path = dir.path().join("Movie.2024.Extras.mkv");
        tokio::fs::write(&main_path, b"movie main").await?;
        tokio::fs::write(&extra_path, b"movie extra").await?;
        let (subscription_id, target_id) =
            create_movie_subscription_with_target(&database, "Movie", 2024).await?;

        let (_release_id, job_id) = insert_completed_release_with_files(
            &database,
            subscription_id,
            MediaType::Movie,
            "Movie.2024.1080p.WEB-DL-GROUP",
            ReleaseKind::Single,
            ReleaseResolverKind::MovieRadarrStyle,
            DEBRID_DEFAULT_LOGICAL_ID,
            "rrm6-movie-selected-coverage",
            vec![
                TestReleaseFile {
                    local_path: main_path.to_string_lossy().to_string(),
                    basename: "Movie.2024.1080p.mkv".to_string(),
                    file_index: 0,
                    selected: Some(true),
                    parsed_season: None,
                    parsed_episode: None,
                    coverage: vec![(
                        target_id,
                        ReleaseCoverageKind::Movie,
                        ReleaseCoverageState::Submitted,
                    )],
                },
                TestReleaseFile {
                    local_path: extra_path.to_string_lossy().to_string(),
                    basename: "Movie.2024.Extras.mkv".to_string(),
                    file_index: 1,
                    selected: Some(true),
                    parsed_season: None,
                    parsed_episode: None,
                    coverage: Vec::new(),
                },
            ],
        )
        .await?;

        let stats = run_acquisition_import_iteration(&database.pool, 10).await?;
        assert_eq!(stats.runs_imported, 1);
        assert_eq!(stats.links_imported, 1);
        let run = get_import_run_by_release_job(&database.pool, job_id)
            .await?
            .expect("import run");
        let links = list_import_file_links(&database.pool, run.import_run_id).await?;
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].local_path.as_deref(), main_path.to_str());
        assert!(links[0].movie_id.is_some());
        let media_files: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_files")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(media_files, 1);
        Ok(())
    }

    #[tokio::test]
    async fn rrm6_movie_import_blocks_placeholder_only_coverage_before_linking() -> Result<()> {
        let database = setup_db().await?;
        let dir = tempdir()?;
        let path = dir.path().join("Movie.2024.1080p.mkv");
        tokio::fs::write(&path, b"movie").await?;
        let (subscription_id, target_id) =
            create_movie_subscription_with_target(&database, "Movie", 2024).await?;

        let (release_id, job_id) = insert_completed_release_with_files(
            &database,
            subscription_id,
            MediaType::Movie,
            "Movie.2024.1080p.WEB-DL-GROUP",
            ReleaseKind::Single,
            ReleaseResolverKind::MovieRadarrStyle,
            DEBRID_DEFAULT_LOGICAL_ID,
            "rrm6-movie-placeholder-only",
            vec![TestReleaseFile {
                local_path: path.to_string_lossy().to_string(),
                basename: "Movie.2024.1080p.mkv".to_string(),
                file_index: 0,
                selected: Some(true),
                parsed_season: None,
                parsed_episode: None,
                coverage: Vec::new(),
            }],
        )
        .await?;
        upsert_release_coverage(
            &database.pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id,
                release_file_id: None,
                target_id,
                coverage_kind: ReleaseCoverageKind::Movie,
                confidence: ReleaseConfidence::High,
                score: Some(1.0),
                reason: Some("placeholder movie coverage".to_string()),
                state: ReleaseCoverageState::Submitted,
                verified_by: Some("test".to_string()),
            },
        )
        .await?;

        let stats = run_acquisition_import_iteration(&database.pool, 10).await?;
        assert_eq!(stats.runs_imported, 0);
        assert_eq!(stats.blocked_runs, 1);
        let run = get_import_run_by_release_job(&database.pool, job_id)
            .await?
            .expect("import run");
        assert_eq!(run.state, AcquisitionImportRunState::Blocked);
        let links = list_import_file_links(&database.pool, run.import_run_id).await?;
        assert_eq!(links.len(), 1);
        assert_eq!(
            links[0].mismatch_class.as_deref(),
            Some("missing_release_file_mapping")
        );
        let media_files: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_files")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(media_files, 0);
        let target = get_target(&database.pool, target_id)
            .await?
            .expect("target");
        assert_eq!(target.state, AcquisitionTargetState::Submitted);
        Ok(())
    }

    #[tokio::test]
    async fn rrm6_movie_import_provenance_preserves_parser_file_selection_and_route_evidence()
    -> Result<()> {
        let database = setup_db().await?;
        let dir = tempdir()?;
        let path = dir.path().join("Movie.2024.1080p.mkv");
        tokio::fs::write(&path, b"movie").await?;
        let (subscription_id, target_id) =
            create_movie_subscription_with_target(&database, "Movie", 2024).await?;
        let source_plan = json!({
            "parsedRelease": {
                "movieTitle": "Movie",
                "year": 2024
            },
            "graph": {
                "targetTitle": "Movie",
                "targetYear": 2024
            },
            "reconciliation": {
                "outcome": "planned",
                "confidence": "high",
                "reviewReasons": []
            }
        });
        let active_plan = json!({
            "source": "debrid_provider_file_list",
            "movie": {
                "coverageKind": "movie",
                "fileSelection": {
                    "policyVersion": "rrm5-movie-main-file-selection-v1",
                    "status": "approved",
                    "selectedFileId": "0",
                    "skippedFileIds": ["1"],
                    "diagnostics": [{
                        "fileId": "0",
                        "path": "Movie.2024.1080p.mkv",
                        "role": "main_candidate",
                        "selected": true
                    }]
                }
            },
            "selectionPolicy": {
                "status": "approved",
                "selectedFileIds": ["0"],
                "skippedFileIds": ["1"]
            },
            "debridRuntime": {
                "status": "completed",
                "remoteReleaseId": "remote-movie-1"
            }
        });
        let (release_id, job_id) = insert_completed_release_with_files_and_plan(
            &database,
            subscription_id,
            MediaType::Movie,
            "Movie.2024.1080p.WEB-DL-GROUP",
            ReleaseKind::Single,
            ReleaseResolverKind::MovieRadarrStyle,
            DEBRID_DEFAULT_LOGICAL_ID,
            "rrm6-movie-provenance",
            vec![TestReleaseFile {
                local_path: path.to_string_lossy().to_string(),
                basename: "Movie.2024.1080p.mkv".to_string(),
                file_index: 0,
                selected: Some(true),
                parsed_season: None,
                parsed_episode: None,
                coverage: vec![(
                    target_id,
                    ReleaseCoverageKind::Movie,
                    ReleaseCoverageState::Submitted,
                )],
            }],
            Some(active_plan),
        )
        .await?;
        sqlx::query(
            "UPDATE acquisition_releases SET selected_candidate_json = ? WHERE release_id = ?",
        )
        .bind(serde_json::to_string(&json!({
            "title": "Movie.2024.1080p.WEB-DL-GROUP",
            "movieCoveragePlan": source_plan,
            "submissionResult": {
                "routeLogicalId": DEBRID_DEFAULT_LOGICAL_ID,
                "downloadId": "download-rrm6-movie-provenance"
            }
        }))?)
        .bind(release_id.to_string())
        .execute(&database.pool)
        .await?;

        let stats = run_acquisition_import_iteration(&database.pool, 10).await?;
        assert_eq!(stats.runs_imported, 1);
        let run = get_import_run_by_release_job(&database.pool, job_id)
            .await?
            .expect("import run");
        assert_eq!(
            run.provenance
                .as_ref()
                .and_then(|value| value.pointer("/movieImportEvidence/parsedRelease/movieTitle"))
                .and_then(JsonValue::as_str),
            Some("Movie")
        );
        assert_eq!(
            run.provenance
                .as_ref()
                .and_then(|value| value.pointer("/movieImportEvidence/fileSelection/status"))
                .and_then(JsonValue::as_str),
            Some("approved")
        );
        assert_eq!(
            run.provenance
                .as_ref()
                .and_then(|value| value.pointer("/movieImportEvidence/route/routeLogicalId"))
                .and_then(JsonValue::as_str),
            Some(DEBRID_DEFAULT_LOGICAL_ID)
        );
        let links = list_import_file_links(&database.pool, run.import_run_id).await?;
        assert_eq!(
            links[0]
                .evidence
                .as_ref()
                .and_then(|value| {
                    value.pointer("/previousEvidence/movieImportEvidence/parsedRelease/year")
                })
                .and_then(JsonValue::as_i64),
            Some(2024)
        );
        assert_eq!(
            links[0]
                .evidence
                .as_ref()
                .and_then(|value| value.pointer("/previousEvidence/file/parsed/title"))
                .and_then(JsonValue::as_str),
            Some("Movie.2024.1080p.WEB-DL-GROUP")
        );
        Ok(())
    }

    #[tokio::test]
    async fn es6_import_provenance_preserves_extension_suite_source_evidence() -> Result<()> {
        let database = setup_db().await?;
        let dir = tempdir()?;
        let path = dir.path().join("Primer.2004.1080p.mkv");
        tokio::fs::write(&path, b"movie").await?;
        let (subscription_id, target_id) =
            create_movie_subscription_with_target(&database, "Primer", 2004).await?;
        let source_provider_id = Uuid::new_v4();
        let secondary_provider_id = Uuid::new_v4();
        let coverage_plan = json!({
            "resolverKind": "movie_radarr_style",
            "requestScopeEvidence": {
                "sourceSelection": {
                    "mode": "suite",
                    "route": "suite:default",
                    "suiteId": "default"
                }
            }
        });
        let (release_id, job_id) = insert_completed_release_with_files_and_plan(
            &database,
            subscription_id,
            MediaType::Movie,
            "Primer.2004.1080p.WEB-DL-GROUP",
            ReleaseKind::Single,
            ReleaseResolverKind::MovieRadarrStyle,
            DEBRID_DEFAULT_LOGICAL_ID,
            "es6-suite-import-provenance",
            vec![TestReleaseFile {
                local_path: path.to_string_lossy().to_string(),
                basename: "Primer.2004.1080p.mkv".to_string(),
                file_index: 0,
                selected: Some(true),
                parsed_season: None,
                parsed_episode: None,
                coverage: vec![(
                    target_id,
                    ReleaseCoverageKind::Movie,
                    ReleaseCoverageState::Submitted,
                )],
            }],
            Some(coverage_plan),
        )
        .await?;
        sqlx::query(
            "UPDATE acquisition_releases SET selected_candidate_json = ? WHERE release_id = ?",
        )
        .bind(serde_json::to_string(&json!({
            "title": "Primer.2004.1080p.WEB-DL-GROUP",
            "sourceProviderId": source_provider_id.to_string(),
            "sourceExtensionId": "elixir.sources.torrentio",
            "sourceSuite": {
                "label": "Elixir Extension Suite",
                "mode": "suite",
                "sourceSelection": {
                    "mode": "suite",
                    "route": "suite:default",
                    "suiteId": "default"
                },
                "primaryProviderId": source_provider_id.to_string(),
                "primaryExtensionId": "elixir.sources.torrentio",
                "contributorCount": 2,
                "contributors": [
                    {
                        "primary": true,
                        "providerId": source_provider_id.to_string(),
                        "extensionId": "elixir.sources.torrentio",
                        "extensionName": "Torrentio",
                        "implementation": "torrentio"
                    },
                    {
                        "primary": false,
                        "providerId": secondary_provider_id.to_string(),
                        "extensionId": "elixir.sources.comet",
                        "extensionName": "Comet",
                        "implementation": "comet"
                    }
                ]
            }
        }))?)
        .bind(release_id.to_string())
        .execute(&database.pool)
        .await?;

        let stats = run_acquisition_import_iteration(&database.pool, 10).await?;
        assert_eq!(stats.runs_imported, 1);
        let run = get_import_run_by_release_job(&database.pool, job_id)
            .await?
            .expect("import run");
        assert_eq!(
            run.provenance
                .as_ref()
                .and_then(|value| value.pointer("/selectedCandidate/sourceSuite/label"))
                .and_then(JsonValue::as_str),
            Some("Elixir Extension Suite")
        );
        assert_eq!(
            run.provenance
                .as_ref()
                .and_then(|value| {
                    value.pointer("/selectedCandidate/sourceSuite/contributors/1/providerId")
                })
                .and_then(JsonValue::as_str)
                .and_then(|value| Uuid::parse_str(value).ok()),
            Some(secondary_provider_id)
        );
        assert_eq!(
            run.provenance
                .as_ref()
                .and_then(|value| {
                    value.pointer("/coveragePlan/requestScopeEvidence/sourceSelection/mode")
                })
                .and_then(JsonValue::as_str),
            Some("suite")
        );
        Ok(())
    }

    #[tokio::test]
    async fn rr8b_tv_season_pack_imports_many_episode_targets() -> Result<()> {
        let database = setup_db().await?;
        let dir = tempdir()?;
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Series,
                title: "Pack Show".to_string(),
                year: Some(2024),
                external_ids: None,
                idempotency_key: None,
                request_mode: None,
                request_scope: None,
                scope: None,
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: AcquisitionMonitorPolicy::SelectedTargets,
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: None,
                release_delay_seconds: Some(0),
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
            },
        )
        .await?;
        let targets = upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            (1..=4)
                .map(|episode| NewAcquisitionTarget {
                    target_key: Some(format!("S01E{episode:02}")),
                    media_type: Some(MediaType::Series),
                    title: Some(format!("Episode {episode}")),
                    season_number: Some(1),
                    episode_number: Some(episode),
                    absolute_episode_number: None,
                    air_date: None,
                    air_time: None,
                    metadata: if episode == 1 || episode == 4 {
                        Some(json!({
                            "source": "tvdb",
                            "tvdbEpisodeId": format!("tvdb-episode-{episode}"),
                            "raw": {
                                "id": 1000 + episode,
                                "name": format!("Episode {episode}"),
                                "overview": format!("Episode {episode} overview."),
                                "image": format!("https://example.invalid/episode-{episode}.jpg"),
                                "runtime": 42
                            }
                        }))
                    } else {
                        None
                    },
                    state: Some(if episode <= 3 {
                        AcquisitionTargetState::Submitted
                    } else {
                        AcquisitionTargetState::Pending
                    }),
                    next_search_after: None,
                })
                .collect(),
        )
        .await?;
        let mut files = Vec::new();
        for target in targets
            .iter()
            .filter(|target| target.episode_number <= Some(3))
        {
            let episode = target.episode_number.expect("episode");
            let basename = format!("Pack.Show.S01E{episode:02}.mkv");
            let path = dir.path().join(&basename);
            tokio::fs::write(&path, b"episode").await?;
            files.push(TestReleaseFile {
                local_path: path.to_string_lossy().to_string(),
                basename,
                file_index: episode as i64,
                selected: Some(true),
                parsed_season: Some(1),
                parsed_episode: Some(episode),
                coverage: vec![(
                    target.target_id,
                    ReleaseCoverageKind::SeasonPack,
                    ReleaseCoverageState::Submitted,
                )],
            });
        }
        let (_release_id, _job_id) = insert_completed_release_with_files(
            &database,
            subscription.subscription_id,
            MediaType::Series,
            "Pack.Show.S01.1080p.WEB-DL-GROUP",
            ReleaseKind::SeasonPack,
            ReleaseResolverKind::TvSonarrStyle,
            DEBRID_DEFAULT_LOGICAL_ID,
            "season-pack-1",
            files,
        )
        .await?;

        let stats = run_acquisition_import_iteration(&database.pool, 10).await?;
        assert_eq!(stats.runs_imported, 1);
        assert_eq!(stats.links_imported, 3);
        let media_files: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_files")
            .fetch_one(&database.pool)
            .await?;
        let episode_files: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM episode_files")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(media_files, 3);
        assert_eq!(episode_files, 3);
        let episode_metadata: String = sqlx::query_scalar(
            "SELECT metadata_json FROM episodes WHERE season_number = 1 AND episode_number = 1",
        )
        .fetch_one(&database.pool)
        .await?;
        let episode_metadata: JsonValue = serde_json::from_str(&episode_metadata)?;
        assert_eq!(
            episode_metadata.get("overview").and_then(JsonValue::as_str),
            Some("Episode 1 overview.")
        );
        assert_eq!(
            episode_metadata.get("image").and_then(JsonValue::as_str),
            Some("https://example.invalid/episode-1.jpg")
        );
        let scaffolded_missing: (i64, String, String) = sqlx::query_as(
            "SELECT CAST(has_file AS INTEGER), title, metadata_json
             FROM episodes
             WHERE season_number = 1 AND episode_number = 4",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(scaffolded_missing.0, 0);
        assert_eq!(scaffolded_missing.1, "Episode 4");
        let missing_metadata: JsonValue = serde_json::from_str(&scaffolded_missing.2)?;
        assert_eq!(
            missing_metadata.get("overview").and_then(JsonValue::as_str),
            Some("Episode 4 overview.")
        );
        let runtime_seconds: i64 = sqlx::query_scalar(
            "SELECT runtime_seconds FROM episodes WHERE season_number = 1 AND episode_number = 1",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(runtime_seconds, 2520);
        for target in targets
            .iter()
            .filter(|target| target.episode_number <= Some(3))
        {
            let imported = get_target(&database.pool, target.target_id)
                .await?
                .expect("target");
            assert_eq!(imported.state, AcquisitionTargetState::Imported);
        }
        Ok(())
    }

    #[tokio::test]
    async fn rr8b_multi_episode_single_file_links_one_media_file_to_multiple_episodes() -> Result<()>
    {
        let database = setup_db().await?;
        let dir = tempdir()?;
        let path = dir.path().join("Double.Show.S01E01-E02.mkv");
        tokio::fs::write(&path, b"double").await?;
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Series,
                title: "Double Show".to_string(),
                year: Some(2024),
                external_ids: None,
                idempotency_key: None,
                request_mode: None,
                request_scope: None,
                scope: None,
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: AcquisitionMonitorPolicy::SelectedTargets,
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: None,
                release_delay_seconds: Some(0),
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
            },
        )
        .await?;
        let targets = upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            (1..=2)
                .map(|episode| NewAcquisitionTarget {
                    target_key: Some(format!("S01E{episode:02}")),
                    media_type: Some(MediaType::Series),
                    title: Some(format!("Episode {episode}")),
                    season_number: Some(1),
                    episode_number: Some(episode),
                    absolute_episode_number: None,
                    air_date: None,
                    air_time: None,
                    metadata: None,
                    state: Some(AcquisitionTargetState::Submitted),
                    next_search_after: None,
                })
                .collect(),
        )
        .await?;
        insert_completed_release_with_files(
            &database,
            subscription.subscription_id,
            MediaType::Series,
            "Double.Show.S01E01-E02.1080p.WEB-DL-GROUP",
            ReleaseKind::MultiEpisode,
            ReleaseResolverKind::TvSonarrStyle,
            DEBRID_DEFAULT_LOGICAL_ID,
            "multi-episode-1",
            vec![TestReleaseFile {
                local_path: path.to_string_lossy().to_string(),
                basename: "Double.Show.S01E01-E02.mkv".to_string(),
                file_index: 0,
                selected: Some(true),
                parsed_season: Some(1),
                parsed_episode: Some(1),
                coverage: targets
                    .iter()
                    .map(|target| {
                        (
                            target.target_id,
                            ReleaseCoverageKind::MultiEpisodeRange,
                            ReleaseCoverageState::Submitted,
                        )
                    })
                    .collect(),
            }],
        )
        .await?;

        let stats = run_acquisition_import_iteration(&database.pool, 10).await?;
        assert_eq!(stats.runs_imported, 1);
        assert_eq!(stats.links_imported, 2);
        let media_files: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_files")
            .fetch_one(&database.pool)
            .await?;
        let episode_files: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM episode_files")
            .fetch_one(&database.pool)
            .await?;
        let distinct_media_files: i64 =
            sqlx::query_scalar("SELECT COUNT(DISTINCT media_file_id) FROM episode_files")
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(media_files, 1);
        assert_eq!(episode_files, 2);
        assert_eq!(distinct_media_files, 1);
        Ok(())
    }

    #[tokio::test]
    async fn rr8b_rejects_competing_release_for_already_imported_target() -> Result<()> {
        let dir = tempdir()?;
        let first_path = dir.path().join("Show.S01E01.first.mkv");
        tokio::fs::write(&first_path, b"first").await?;
        let fixture = setup_completed_release(
            DEBRID_DEFAULT_LOGICAL_ID,
            Some(first_path.to_string_lossy().to_string()),
            ReleaseCoverageState::Submitted,
            Some(true),
        )
        .await?;
        run_acquisition_import_iteration(&fixture.database.pool, 10).await?;

        let target = get_target(&fixture.database.pool, fixture.target_id)
            .await?
            .expect("target");
        let second_path = dir.path().join("Show.S01E01.second.mkv");
        tokio::fs::write(&second_path, b"second").await?;
        let (_release_id, second_job_id) = insert_completed_release_with_files(
            &fixture.database,
            target.subscription_id,
            MediaType::Series,
            "Show.S01E01.REPACK.1080p.WEB-DL-GROUP",
            ReleaseKind::Single,
            ReleaseResolverKind::TvSonarrStyle,
            DEBRID_DEFAULT_LOGICAL_ID,
            "competing-1",
            vec![TestReleaseFile {
                local_path: second_path.to_string_lossy().to_string(),
                basename: "Show.S01E01.second.mkv".to_string(),
                file_index: 0,
                selected: Some(true),
                parsed_season: Some(1),
                parsed_episode: Some(1),
                coverage: vec![(
                    fixture.target_id,
                    ReleaseCoverageKind::SingleEpisode,
                    ReleaseCoverageState::Submitted,
                )],
            }],
        )
        .await?;

        let stats = run_acquisition_import_iteration(&fixture.database.pool, 10).await?;
        assert_eq!(stats.runs_imported, 0);
        assert_eq!(stats.blocked_runs, 1);
        let run = get_import_run_by_release_job(&fixture.database.pool, second_job_id)
            .await?
            .expect("second import run");
        assert_eq!(run.state, AcquisitionImportRunState::Blocked);
        assert_eq!(
            run.mismatch_class.as_deref(),
            Some("target_already_imported")
        );
        let media_files: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_files")
            .fetch_one(&fixture.database.pool)
            .await?;
        assert_eq!(media_files, 1);
        Ok(())
    }

    #[tokio::test]
    async fn rr8c_anime_hash_match_imports_selected_file() -> Result<()> {
        let database = setup_db().await?;
        let dir = tempdir()?;
        let path = dir.path().join("Anime.Show.S01E01.mkv");
        tokio::fs::write(&path, b"anime episode one").await?;
        seed_anidb_hit_for_file(&database, &path, vec![2001]).await?;
        let (subscription, targets) =
            create_anime_subscription_with_targets(&database, "Anime Show", &[2001]).await?;

        let (release_id, job_id) = insert_completed_release_with_files_and_plan(
            &database,
            subscription.subscription_id,
            MediaType::Anime,
            "Anime.Show.01.1080p.WEB-DL-GRP",
            ReleaseKind::Single,
            ReleaseResolverKind::AnimeShokoStyle,
            DEBRID_DEFAULT_LOGICAL_ID,
            "anime-match-1",
            vec![TestReleaseFile {
                local_path: path.to_string_lossy().to_string(),
                basename: "Anime.Show.S01E01.mkv".to_string(),
                file_index: 0,
                selected: Some(true),
                parsed_season: Some(1),
                parsed_episode: Some(1),
                coverage: vec![(
                    targets[0].target_id,
                    ReleaseCoverageKind::SingleEpisode,
                    ReleaseCoverageState::Submitted,
                )],
            }],
            Some(json!({
                "fingerprint": "anime-match-1",
                "animeVerification": { "requireHashBeforeImport": true }
            })),
        )
        .await?;

        let stats = run_acquisition_import_iteration(&database.pool, 10).await?;
        assert_eq!(stats.runs_imported, 1);
        assert_eq!(stats.links_imported, 1);
        let run = get_import_run_by_release_job(&database.pool, job_id)
            .await?
            .expect("import run");
        assert_eq!(run.state, AcquisitionImportRunState::Imported);
        let links = list_import_file_links(&database.pool, run.import_run_id).await?;
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].verification_state.as_deref(), Some("verified"));
        assert!(links[0].media_file_id.is_some());
        let attempts = list_anime_match_attempts_by_release(&database.pool, release_id).await?;
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].outcome, AnimeMatchOutcome::Verified);
        let mismatches =
            list_anime_identity_mismatches_by_release(&database.pool, release_id).await?;
        assert!(mismatches.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn rr8c_verified_wrong_episode_quarantines_before_library_link() -> Result<()> {
        let database = setup_db().await?;
        let dir = tempdir()?;
        let path = dir.path().join("Anime.Show.S01E01.bad.mkv");
        tokio::fs::write(&path, b"anime wrong episode").await?;
        seed_anidb_hit_for_file(&database, &path, vec![2999]).await?;
        let (subscription, targets) =
            create_anime_subscription_with_targets(&database, "Anime Show", &[2001]).await?;

        let (release_id, job_id) = insert_completed_release_with_files_and_plan(
            &database,
            subscription.subscription_id,
            MediaType::Anime,
            "Anime.Show.01.1080p.WEB-DL-GRP",
            ReleaseKind::Single,
            ReleaseResolverKind::AnimeShokoStyle,
            DEBRID_DEFAULT_LOGICAL_ID,
            "anime-mismatch-1",
            vec![TestReleaseFile {
                local_path: path.to_string_lossy().to_string(),
                basename: "Anime.Show.S01E01.bad.mkv".to_string(),
                file_index: 0,
                selected: Some(true),
                parsed_season: Some(1),
                parsed_episode: Some(1),
                coverage: vec![(
                    targets[0].target_id,
                    ReleaseCoverageKind::SingleEpisode,
                    ReleaseCoverageState::Submitted,
                )],
            }],
            Some(json!({
                "fingerprint": "anime-mismatch-1",
                "animeVerification": { "requireHashBeforeImport": true }
            })),
        )
        .await?;

        let stats = run_acquisition_import_iteration(&database.pool, 10).await?;
        assert_eq!(stats.runs_imported, 0);
        assert_eq!(stats.blocked_runs, 1);
        let run = get_import_run_by_release_job(&database.pool, job_id)
            .await?
            .expect("import run");
        assert_eq!(run.state, AcquisitionImportRunState::Mismatched);
        assert_eq!(
            run.mismatch_class.as_deref(),
            Some("anime_hash_identity_mismatch")
        );
        let release_state: String =
            sqlx::query_scalar("SELECT state FROM acquisition_releases WHERE release_id = ?")
                .bind(release_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(
            release_state,
            AcquisitionReleaseState::ReviewRequired.as_str()
        );
        let media_files: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_files")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(media_files, 0);
        let mismatches =
            list_anime_identity_mismatches_by_release(&database.pool, release_id).await?;
        assert_eq!(mismatches.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn rr8c_no_anidb_budget_defers_without_source_rediscovery() -> Result<()> {
        let database = setup_db().await?;
        reserve_anidb_channel_request(
            &database.pool,
            AniDbChannel::Udp,
            &AniDbRateLimiterConfig::default(),
            Utc::now(),
        )
        .await?;
        let dir = tempdir()?;
        let path = dir.path().join("Anime.Show.S01E01.pending.mkv");
        tokio::fs::write(&path, b"anime pending identity").await?;
        let (subscription, targets) =
            create_anime_subscription_with_targets(&database, "Anime Show", &[2001]).await?;

        let (release_id, job_id) = insert_completed_release_with_files_and_plan(
            &database,
            subscription.subscription_id,
            MediaType::Anime,
            "Anime.Show.01.1080p.WEB-DL-GRP",
            ReleaseKind::Single,
            ReleaseResolverKind::AnimeShokoStyle,
            DEBRID_DEFAULT_LOGICAL_ID,
            "anime-deferred-1",
            vec![TestReleaseFile {
                local_path: path.to_string_lossy().to_string(),
                basename: "Anime.Show.S01E01.pending.mkv".to_string(),
                file_index: 0,
                selected: Some(true),
                parsed_season: Some(1),
                parsed_episode: Some(1),
                coverage: vec![(
                    targets[0].target_id,
                    ReleaseCoverageKind::SingleEpisode,
                    ReleaseCoverageState::Submitted,
                )],
            }],
            Some(json!({
                "fingerprint": "anime-deferred-1",
                "animeVerification": { "requireHashBeforeImport": true }
            })),
        )
        .await?;

        let stats = run_acquisition_import_iteration(&database.pool, 10).await?;
        assert_eq!(stats.runs_imported, 0);
        assert_eq!(stats.blocked_runs, 1);
        let run = get_import_run_by_release_job(&database.pool, job_id)
            .await?
            .expect("import run");
        assert_eq!(run.state, AcquisitionImportRunState::Blocked);
        assert_eq!(
            run.mismatch_class.as_deref(),
            Some("anidb_lookup_rate_limited")
        );
        let release_state: String =
            sqlx::query_scalar("SELECT state FROM acquisition_releases WHERE release_id = ?")
                .bind(release_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(release_state, AcquisitionReleaseState::Completed.as_str());
        let target = get_target(&database.pool, targets[0].target_id)
            .await?
            .expect("target");
        assert_eq!(target.state, AcquisitionTargetState::Submitted);
        let media_files: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_files")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(media_files, 0);
        Ok(())
    }

    #[tokio::test]
    async fn rr8c_cached_hash_identity_avoids_anidb_gate() -> Result<()> {
        let database = setup_db().await?;
        let dir = tempdir()?;
        let path = dir.path().join("Anime.Show.S01E01.cached.mkv");
        tokio::fs::write(&path, b"anime cached identity").await?;
        seed_anidb_hit_for_file(&database, &path, vec![2001]).await?;
        let (subscription, targets) =
            create_anime_subscription_with_targets(&database, "Anime Show", &[2001]).await?;

        insert_completed_release_with_files_and_plan(
            &database,
            subscription.subscription_id,
            MediaType::Anime,
            "Anime.Show.01.1080p.WEB-DL-GRP",
            ReleaseKind::Single,
            ReleaseResolverKind::AnimeShokoStyle,
            DEBRID_DEFAULT_LOGICAL_ID,
            "anime-cache-1",
            vec![TestReleaseFile {
                local_path: path.to_string_lossy().to_string(),
                basename: "Anime.Show.S01E01.cached.mkv".to_string(),
                file_index: 0,
                selected: Some(true),
                parsed_season: Some(1),
                parsed_episode: Some(1),
                coverage: vec![(
                    targets[0].target_id,
                    ReleaseCoverageKind::SingleEpisode,
                    ReleaseCoverageState::Submitted,
                )],
            }],
            Some(json!({
                "fingerprint": "anime-cache-1",
                "animeVerification": { "requireHashBeforeImport": true }
            })),
        )
        .await?;

        let stats = run_acquisition_import_iteration(&database.pool, 10).await?;
        assert_eq!(stats.runs_imported, 1);
        let channel_states: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM acquisition_anidb_channel_state")
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(channel_states, 0);
        Ok(())
    }

    #[tokio::test]
    async fn rr8c_user_approved_repair_imports_with_override_evidence() -> Result<()> {
        let database = setup_db().await?;
        let dir = tempdir()?;
        let path = dir.path().join("Anime.Show.S01E01.override.mkv");
        tokio::fs::write(&path, b"anime override").await?;
        let (subscription, targets) =
            create_anime_subscription_with_targets(&database, "Anime Show", &[2001]).await?;

        let (_release_id, job_id) = insert_completed_release_with_files_and_plan(
            &database,
            subscription.subscription_id,
            MediaType::Anime,
            "Anime.Show.01.1080p.WEB-DL-GRP",
            ReleaseKind::Single,
            ReleaseResolverKind::AnimeShokoStyle,
            DEBRID_DEFAULT_LOGICAL_ID,
            "anime-override-1",
            vec![TestReleaseFile {
                local_path: path.to_string_lossy().to_string(),
                basename: "Anime.Show.S01E01.override.mkv".to_string(),
                file_index: 0,
                selected: Some(true),
                parsed_season: Some(1),
                parsed_episode: Some(1),
                coverage: vec![(
                    targets[0].target_id,
                    ReleaseCoverageKind::ManualOverride,
                    ReleaseCoverageState::Submitted,
                )],
            }],
            Some(json!({
                "fingerprint": "anime-override-1",
                "manualReview": { "userApproved": true },
                "animeVerification": {
                    "requireHashBeforeImport": true,
                    "userApprovedImportOverride": true
                }
            })),
        )
        .await?;

        let stats = run_acquisition_import_iteration(&database.pool, 10).await?;
        assert_eq!(stats.runs_imported, 1);
        assert_eq!(stats.links_imported, 1);
        let run = get_import_run_by_release_job(&database.pool, job_id)
            .await?
            .expect("import run");
        let links = list_import_file_links(&database.pool, run.import_run_id).await?;
        assert_eq!(
            links[0].verification_state.as_deref(),
            Some("manual_override")
        );
        assert_eq!(
            run.provenance
                .as_ref()
                .and_then(|value| value.get("manualCoverageMappings"))
                .and_then(JsonValue::as_array)
                .map(Vec::len),
            Some(1)
        );
        assert_eq!(
            run.provenance
                .as_ref()
                .and_then(|value| value.get("manualReview"))
                .and_then(|value| value.get("userApproved"))
                .and_then(JsonValue::as_bool),
            Some(true)
        );
        assert_eq!(
            count_acquisition_audit_events(
                &database.pool,
                _release_id,
                EVENT_MANUAL_IMPORT_COMPLETED,
            )
            .await?,
            1
        );
        let file_hashes: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM acquisition_file_hashes")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(file_hashes, 0);
        Ok(())
    }

    #[tokio::test]
    async fn amr4_manual_anime_mapping_without_verification_override_quarantines_wrong_file()
    -> Result<()> {
        let database = setup_db().await?;
        let dir = tempdir()?;
        let path = dir.path().join("Anime.Show.S01E01.manual-wrong.mkv");
        tokio::fs::write(&path, b"anime manual wrong episode").await?;
        seed_anidb_hit_for_file(&database, &path, vec![2999]).await?;
        let (subscription, targets) =
            create_anime_subscription_with_targets(&database, "Anime Show", &[2001]).await?;

        let (release_id, job_id) = insert_completed_release_with_files_and_plan(
            &database,
            subscription.subscription_id,
            MediaType::Anime,
            "Anime.Show.01.1080p.WEB-DL-GRP",
            ReleaseKind::Single,
            ReleaseResolverKind::AnimeShokoStyle,
            DEBRID_DEFAULT_LOGICAL_ID,
            "anime-manual-mismatch-1",
            vec![TestReleaseFile {
                local_path: path.to_string_lossy().to_string(),
                basename: "Anime.Show.S01E01.manual-wrong.mkv".to_string(),
                file_index: 0,
                selected: Some(true),
                parsed_season: Some(1),
                parsed_episode: Some(1),
                coverage: vec![(
                    targets[0].target_id,
                    ReleaseCoverageKind::ManualOverride,
                    ReleaseCoverageState::Submitted,
                )],
            }],
            Some(json!({
                "fingerprint": "anime-manual-mismatch-1",
                "manualReview": { "userApproved": true },
                "animeVerification": { "requireHashBeforeImport": true }
            })),
        )
        .await?;

        let stats = run_acquisition_import_iteration(&database.pool, 10).await?;
        assert_eq!(stats.runs_imported, 0);
        assert_eq!(stats.blocked_runs, 1);
        let run = get_import_run_by_release_job(&database.pool, job_id)
            .await?
            .expect("import run");
        assert_eq!(run.state, AcquisitionImportRunState::Mismatched);
        assert_eq!(
            run.mismatch_class.as_deref(),
            Some("anime_hash_identity_mismatch")
        );
        let release_state: String =
            sqlx::query_scalar("SELECT state FROM acquisition_releases WHERE release_id = ?")
                .bind(release_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(
            release_state,
            AcquisitionReleaseState::ReviewRequired.as_str()
        );
        let media_files: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_files")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(media_files, 0);
        assert_eq!(
            count_acquisition_audit_events(
                &database.pool,
                release_id,
                EVENT_MANUAL_IMPORT_QUARANTINED,
            )
            .await?,
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn rr8c_hash_mismatch_leaves_unrelated_targets_unmodified() -> Result<()> {
        let database = setup_db().await?;
        let dir = tempdir()?;
        let path = dir.path().join("Anime.Show.S01E01.wrong.mkv");
        tokio::fs::write(&path, b"anime mismatched in pack").await?;
        seed_anidb_hit_for_file(&database, &path, vec![2002]).await?;
        let (subscription, targets) =
            create_anime_subscription_with_targets(&database, "Anime Show", &[2001, 2002]).await?;

        let (release_id, _job_id) = insert_completed_release_with_files_and_plan(
            &database,
            subscription.subscription_id,
            MediaType::Anime,
            "Anime.Show.01.1080p.WEB-DL-GRP",
            ReleaseKind::Single,
            ReleaseResolverKind::AnimeShokoStyle,
            DEBRID_DEFAULT_LOGICAL_ID,
            "anime-unrelated-1",
            vec![TestReleaseFile {
                local_path: path.to_string_lossy().to_string(),
                basename: "Anime.Show.S01E01.wrong.mkv".to_string(),
                file_index: 0,
                selected: Some(true),
                parsed_season: Some(1),
                parsed_episode: Some(1),
                coverage: vec![(
                    targets[0].target_id,
                    ReleaseCoverageKind::SingleEpisode,
                    ReleaseCoverageState::Submitted,
                )],
            }],
            Some(json!({
                "fingerprint": "anime-unrelated-1",
                "animeVerification": { "requireHashBeforeImport": true }
            })),
        )
        .await?;

        let stats = run_acquisition_import_iteration(&database.pool, 10).await?;
        assert_eq!(stats.runs_imported, 0);
        assert_eq!(stats.blocked_runs, 1);
        for target in targets {
            let persisted = get_target(&database.pool, target.target_id)
                .await?
                .expect("target");
            assert_eq!(persisted.state, AcquisitionTargetState::Submitted);
            assert!(persisted.import_event_id.is_none());
        }
        let mismatches =
            list_anime_identity_mismatches_by_release(&database.pool, release_id).await?;
        assert_eq!(mismatches.len(), 1);
        let media_files: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_files")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(media_files, 0);
        Ok(())
    }

    #[tokio::test]
    async fn rr8a_unselected_and_review_required_files_are_ignored() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("Show.S01E01.mkv");
        let fixture = setup_completed_release(
            DEBRID_DEFAULT_LOGICAL_ID,
            Some(path.to_string_lossy().to_string()),
            ReleaseCoverageState::ReviewRequired,
            Some(false),
        )
        .await?;

        let stats = run_acquisition_import_iteration(&fixture.database.pool, 10).await?;
        assert_eq!(stats.runs_created, 1);
        assert_eq!(stats.links_upserted, 0);
        assert_eq!(stats.blocked_runs, 1);
        let run = get_import_run_by_release_job(&fixture.database.pool, fixture.job_id)
            .await?
            .expect("import run");
        assert_eq!(run.state, AcquisitionImportRunState::Blocked);
        let links = list_import_file_links(&fixture.database.pool, run.import_run_id).await?;
        assert!(links.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn osr4_import_ignores_unselected_out_of_scope_pack_files() -> Result<()> {
        let database = setup_db().await?;
        let dir = tempdir()?;
        let wanted_path = dir.path().join("Show.S01E01.mkv");
        let skipped_path = dir.path().join("Show.S01E02.mkv");
        tokio::fs::write(&wanted_path, b"wanted episode").await?;
        tokio::fs::write(&skipped_path, b"skipped episode").await?;
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Series,
                title: "Show".to_string(),
                year: Some(2024),
                external_ids: None,
                idempotency_key: None,
                request_mode: Some(AcquisitionRequestMode::OneShot),
                request_scope: Some(AcquisitionRequestScope::Episode),
                scope: Some(json!({
                    "kind": "episode",
                    "seasonNumber": 1,
                    "episodeNumber": 1,
                    "targetKey": "S01E01"
                })),
                metadata_policy: Some(AcquisitionMetadataPolicy::InitialOnly),
                completion_policy: Some(AcquisitionCompletionPolicy::TerminalSelectedTargets),
                monitor_policy: AcquisitionMonitorPolicy::SelectedTargets,
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: None,
                release_delay_seconds: Some(0),
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
            },
        )
        .await?;
        let targets = upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![NewAcquisitionTarget {
                target_key: Some("S01E01".to_string()),
                media_type: Some(MediaType::Series),
                title: Some("Episode 1".to_string()),
                season_number: Some(1),
                episode_number: Some(1),
                absolute_episode_number: None,
                air_date: None,
                air_time: None,
                metadata: None,
                state: Some(AcquisitionTargetState::Submitted),
                next_search_after: None,
            }],
        )
        .await?;
        let target_id = targets[0].target_id;
        let (_release_id, job_id) = insert_completed_release_with_files_and_plan(
            &database,
            subscription.subscription_id,
            MediaType::Series,
            "Show.S01.COMPLETE.1080p.WEB-DL-GROUP",
            ReleaseKind::SeasonPack,
            ReleaseResolverKind::TvSonarrStyle,
            DEBRID_DEFAULT_LOGICAL_ID,
            "osr4-import-scope",
            vec![
                TestReleaseFile {
                    local_path: wanted_path.to_string_lossy().to_string(),
                    basename: "Show.S01E01.mkv".to_string(),
                    file_index: 0,
                    selected: Some(true),
                    parsed_season: Some(1),
                    parsed_episode: Some(1),
                    coverage: vec![(
                        target_id,
                        ReleaseCoverageKind::SeasonPack,
                        ReleaseCoverageState::Submitted,
                    )],
                },
                TestReleaseFile {
                    local_path: skipped_path.to_string_lossy().to_string(),
                    basename: "Show.S01E02.mkv".to_string(),
                    file_index: 1,
                    selected: Some(false),
                    parsed_season: Some(1),
                    parsed_episode: Some(2),
                    coverage: Vec::new(),
                },
            ],
            Some(json!({
                "requestScopeEvidence": {
                    "requestMode": "one_shot",
                    "requestScope": "episode",
                    "targetCount": 1,
                    "targetKeys": ["S01E01"]
                },
                "selectionPolicy": {
                    "status": "approved",
                    "selectedFileIds": ["0"],
                    "skippedFileIds": ["1"]
                }
            })),
        )
        .await?;

        let stats = run_acquisition_import_iteration(&database.pool, 10).await?;
        assert_eq!(stats.runs_imported, 1);
        let run = get_import_run_by_release_job(&database.pool, job_id)
            .await?
            .expect("import run");
        let links = list_import_file_links(&database.pool, run.import_run_id).await?;
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].target_id, Some(target_id));
        let media_files: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_files")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(media_files, 1);
        let imported_target = get_target(&database.pool, target_id)
            .await?
            .expect("target");
        assert_eq!(imported_target.state, AcquisitionTargetState::Imported);
        Ok(())
    }

    #[tokio::test]
    async fn rr8a_missing_local_file_blocks_without_importing_target() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("missing.mkv");
        let fixture = setup_completed_release(
            DEBRID_DEFAULT_LOGICAL_ID,
            Some(path.to_string_lossy().to_string()),
            ReleaseCoverageState::Submitted,
            Some(true),
        )
        .await?;
        tokio::fs::remove_file(&fixture.local_path).await?;

        let stats = run_acquisition_import_iteration(&fixture.database.pool, 10).await?;
        assert_eq!(stats.blocked_runs, 1);
        let run = get_import_run_by_release_job(&fixture.database.pool, fixture.job_id)
            .await?
            .expect("import run");
        assert_eq!(run.state, AcquisitionImportRunState::Blocked);
        let links = list_import_file_links(&fixture.database.pool, run.import_run_id).await?;
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].state, AcquisitionImportFileLinkState::Blocked);
        assert_eq!(
            links[0].mismatch_class.as_deref(),
            Some("missing_local_file")
        );
        let target = get_target(&fixture.database.pool, fixture.target_id)
            .await?
            .expect("target");
        assert_eq!(target.state, AcquisitionTargetState::Submitted);
        assert!(target.import_event_id.is_none());
        let coverage = list_release_coverage(&fixture.database.pool, fixture.release_id).await?;
        assert_eq!(coverage[0].state, ReleaseCoverageState::Submitted);
        Ok(())
    }

    #[tokio::test]
    async fn rr8a_arr_managed_import_events_are_not_claimed() -> Result<()> {
        let database = setup_db().await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO managed_import_events (
                event_id,
                event_key,
                intent_id,
                media_type,
                manager_provider_id,
                manager_item_id,
                manager_label,
                imported_files_json,
                raw_manager_payload_json,
                status
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind("arr-event-key")
        .bind(Uuid::new_v4().to_string())
        .bind(MediaType::Series.as_str())
        .bind(Uuid::new_v4().to_string())
        .bind("arr-item")
        .bind("Sonarr")
        .bind("[]")
        .bind("{}")
        .bind("pending")
        .execute(&database.pool)
        .await?;

        let stats = run_acquisition_import_iteration(&database.pool, 10).await?;
        assert_eq!(stats.candidates, 0);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM acquisition_import_runs")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(count, 0);
        Ok(())
    }

    #[test]
    fn rr8d_audio_policy_blocks_known_wrong_language() {
        let metadata = media_metadata_with_audio_language(Some("rus"), None);
        let policy = ImportAudioPolicy {
            required_languages: vec!["en".to_string()],
            sources: vec!["qualityProfile.requiredLanguages".to_string()],
        };

        let decision = evaluate_import_audio_policy(&metadata, &policy);

        assert_eq!(
            decision,
            ImportAudioDecision::Block {
                verification_state: "audio_language_mismatch",
                mismatch_class: "audio_language_mismatch",
                reason: "Downloaded file audio languages ru do not include required or claimed language en."
                    .to_string(),
            }
        );
    }

    #[test]
    fn rr8d_audio_policy_allows_unknown_tags_without_false_block() {
        let metadata = media_metadata_with_audio_language(Some("und"), None);
        let policy = ImportAudioPolicy {
            required_languages: vec!["en".to_string()],
            sources: vec!["qualityProfile.requiredLanguages".to_string()],
        };

        assert_eq!(
            evaluate_import_audio_policy(&metadata, &policy),
            ImportAudioDecision::Pass
        );
    }

    #[test]
    fn rr8d_audio_policy_does_not_default_to_english() {
        let metadata = media_metadata_with_audio_language(Some("rus"), None);
        let policy = ImportAudioPolicy {
            required_languages: Vec::new(),
            sources: Vec::new(),
        };

        assert_eq!(
            evaluate_import_audio_policy(&metadata, &policy),
            ImportAudioDecision::Pass
        );
    }

    #[test]
    fn rr8d_import_audio_policy_collects_candidate_and_parsed_language_hints() {
        let subscription = AcquisitionSubscription {
            quality_profile: None,
            ..test_subscription_for_import_policy(MediaType::Series)
        };
        let release = AcquisitionRelease {
            release_id: Uuid::new_v4(),
            subscription_id: Some(subscription.subscription_id),
            source_provider_id: None,
            source_extension_id: "test.source".to_string(),
            owner_id: DEFAULT_ROUTE_OWNER_ID.to_string(),
            media_type: MediaType::Series,
            title: "Show".to_string(),
            release_title: "Show.S01E01.English.1080p-WEB".to_string(),
            source: "magnet:?xt=urn:btih:test".to_string(),
            source_kind: "magnet".to_string(),
            info_hash: Some("test".to_string()),
            fingerprint: "test".to_string(),
            release_kind: ReleaseKind::Single,
            resolver_kind: ReleaseResolverKind::TvSonarrStyle,
            resolver_version: "test".to_string(),
            confidence: ReleaseConfidence::High,
            score: None,
            selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
            selected_provider_id: None,
            download_id: Some("download".to_string()),
            remote_release_id: None,
            state: AcquisitionReleaseState::Completed,
            state_reason: Some("completed".to_string()),
            selected_candidate: Some(json!({ "language": "eng" })),
            coverage_plan: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let release_file = AcquisitionReleaseFile {
            release_file_id: Uuid::new_v4(),
            release_id: release.release_id,
            file_index: Some(0),
            file_id: Some("1".to_string()),
            provider_file_id: Some("1".to_string()),
            path: "Show.S01E01.English.1080p.mkv".to_string(),
            basename: "Show.S01E01.English.1080p.mkv".to_string(),
            size_bytes: Some(1_000),
            selectable: true,
            selected: Some(true),
            parsed_title: Some("Show".to_string()),
            parsed_season_number: Some(1),
            parsed_episode_number: Some(1),
            parsed_episode_end_number: None,
            parsed_absolute_episode_number: None,
            parsed_absolute_episode_end_number: None,
            parsed_air_date: None,
            parsed_quality: Some("1080p".to_string()),
            parsed_language: Some("English".to_string()),
            parsed_release_group: Some("GROUP".to_string()),
            parser_confidence: ReleaseConfidence::High,
            parser_reason: None,
            raw: None,
            provider_metadata: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let policy = import_audio_policy(&subscription, &release, &release_file);

        assert_eq!(policy.required_languages, vec!["en".to_string()]);
        assert!(
            policy
                .sources
                .iter()
                .any(|source| source == "selectedCandidate.language")
        );
        assert!(
            policy
                .sources
                .iter()
                .any(|source| source == "releaseFile.parsedLanguage")
        );
    }

    fn media_metadata_with_audio_language(
        language: Option<&str>,
        title: Option<&str>,
    ) -> ffprobe::MediaMetadata {
        let mut tags = HashMap::new();
        if let Some(language) = language {
            tags.insert("language".to_string(), language.to_string());
        }
        if let Some(title) = title {
            tags.insert("title".to_string(), title.to_string());
        }
        ffprobe::MediaMetadata {
            streams: vec![ffprobe::Stream {
                index: Some(1),
                codec_type: Some("audio".to_string()),
                codec_name: Some("aac".to_string()),
                channels: Some(2),
                tags: Some(tags),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn test_subscription_for_import_policy(media_type: MediaType) -> AcquisitionSubscription {
        AcquisitionSubscription {
            subscription_id: Uuid::new_v4(),
            media_type,
            title: "Show".to_string(),
            normalized_title: "show".to_string(),
            year: Some(2024),
            external_ids: None,
            idempotency_key: None,
            request_mode: AcquisitionRequestMode::Monitored,
            request_scope: AcquisitionRequestScope::Subscription,
            scope: None,
            metadata_policy: AcquisitionMetadataPolicy::Recurring,
            completion_policy: AcquisitionCompletionPolicy::Manual,
            monitor_policy: AcquisitionMonitorPolicy::AllMissing,
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
}
