pub(crate) mod anime_matching_adapter;
mod anime_repair;
mod anime_repair_loop;
mod linkers;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use anyhow::Result;
use chrono::Utc;
use elixir_classifier::hint::anime_parser_adapter::AnimeParserAdapter;
use elixir_classifier::hint::folder_context_parser::FolderContextParser;
use elixir_classifier::hint::general_parser::GeneralParser;
use elixir_classifier::hint::id_extractor_parser::IdExtractorParser;
use elixir_classifier::hint::{
    ClassificationHint as ClassifierHint, FileInput as ClassifierFileInput,
    LibraryType as ClassifierLibraryType,
};
use elixir_classifier::identify::anilist::{AniListIdentifier, AniListRelationNode};
use elixir_classifier::identify::cinemeta::CinemetaIdentifier;
use elixir_classifier::identify::tvdb::TvdbIdentifier;
use elixir_classifier::identify::{
    CandidateScorer, CanonicalMatch as ClassifierCanonicalMatch, DefaultScorer,
    ExternalIds as ClassifierExternalIds, IdentifierProvider,
};
use elixir_classifier::link::anizip_linker::AniZipLinker;
use elixir_classifier::link::tvdb_linker::TvdbLinker;
use elixir_classifier::pipeline::{ClassifiedHint, ClassifierPipeline};
use sqlx::{AnyPool, Row};
use unicode_normalization::UnicodeNormalization;
use uuid::Uuid;

use crate::{
    anime_matching::{
        AnimeMatchAssistProvenance, AnimeMatchAudioPreference, AnimeMatchMediaType,
        AnimeMatchParseFacts, AnimeMatchScope, AnimeMatchTarget, AnimeMatchingService,
        AnimeSemanticMediaKind, build_semantic_evidence_request,
    },
    artwork::{
        ArtworkCandidate, ArtworkKind, ArtworkService, extract_anilist_artwork,
        extract_cinemeta_artwork, extract_tvdb_artworks, extract_tvdb_entity_artwork,
        extract_tvdb_series_artwork,
    },
    config::ClassifierConfig,
    db::models::MediaType,
    extensions::store::{
        ExtensionStore, ManagedEpisodeTombstone, ManagedImportEvent, ManagedImportFile,
        ManagedIngestIntent, ManagedMediaTombstone, NewManagedLibraryProvenance,
    },
    extensions::{ExternalIds, make_identity_key},
    extensions::{FileDescriptor, MediaFileCandidate, MediaIdentity},
    media::ffprobe,
    metadata::{MetadataResult, MetadataService},
    playback::probe as playback_probe,
    state::AppState,
};

use self::anime_matching_adapter::{
    LibraryAnimeMatchFileInput, LibraryAnimeMatchRequestInput, LibraryAnimeMatchSeasonInput,
    build_mapping_targets, library_anime_match_batch_input,
};

pub use anime_repair::{
    ANIME_LIBRARY_REPAIR_VERSION, AnimeLibraryRepairSnapshot, AnimeLibraryRepairTrigger,
    anime_library_repair_snapshot, run_anime_library_repair_for_state,
};
pub use anime_repair_loop::{
    request_anime_library_repair_after_provider_correction,
    request_anime_library_repair_after_scan, start_anime_library_repair_loop,
};
pub use linkers::{AniZipEpisodeRecord, AniZipMapping, LinkerService};

const ANILIST_ENDPOINT: &str = "https://graphql.anilist.co";
const CLASSIFICATION_APPLICATION_CONFIDENCE: f32 = 0.85;
// Product-owned gap calibrated against the ALM-4 anime ambiguity fixtures.
// It deliberately remains one internal policy constant, never a user setting.
const CLASSIFICATION_APPLICATION_MIN_MARGIN: f32 = 0.05;
const ANIZIP_MAPPING_CACHE_SCHEMA_VERSION: i32 = 1;
const APPLIED_CLASSIFICATION_IDENTITY_EVIDENCE_SCHEMA_VERSION: i32 = 2;

// Library scans and the historical repair worker both materialize canonical
// identity. Keep those two production paths mutually exclusive so a scan that
// began from stale in-memory evidence cannot publish partial identity while a
// repair commits. The database-level per-file fences remain the cross-process
// backstop; this lock covers the normal single-server process boundary without
// holding a database transaction across metadata or model I/O.
static LIBRARY_IDENTITY_MUTATION_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));
const LIBRARY_IDENTITY_POSTGRES_ADVISORY_LOCK: i64 = 0x454C_4958_414C_4D38;

async fn acquire_library_identity_database_guard(
    pool: &AnyPool,
) -> Result<Option<sqlx::Transaction<'static, sqlx::Any>>> {
    let mut transaction = pool.begin().await?;
    if transaction.backend_name() != "PostgreSQL" {
        transaction.rollback().await?;
        return Ok(None);
    }
    anyhow::ensure!(
        pool.options().get_max_connections() >= 2,
        "PostgreSQL library identity coordination requires a two-connection pool invariant"
    );
    sqlx::query::<sqlx::Any>("SELECT pg_advisory_xact_lock($1)")
        .bind(LIBRARY_IDENTITY_POSTGRES_ADVISORY_LOCK)
        .execute(&mut *transaction)
        .await?;
    Ok(Some(transaction))
}

pub async fn run_full_scan(
    pool: &AnyPool,
    candidates: Vec<MediaFileCandidate>,
    hash_dedupe: bool,
) -> Result<()> {
    run_full_scan_with_metadata_and_linkers(
        pool,
        None,
        None,
        None,
        None,
        candidates,
        false,
        false,
        hash_dedupe,
    )
    .await
}

pub async fn ingest_managed_import_event(
    pool: &AnyPool,
    metadata: Option<&MetadataService>,
    linkers: Option<&LinkerService>,
    artwork: Option<&ArtworkService>,
    intent: &ManagedIngestIntent,
    event: &ManagedImportEvent,
) -> Result<Option<Uuid>> {
    if event.intent_id != intent.intent_id {
        anyhow::bail!(
            "managed import event {} does not belong to intent {}",
            event.event_id,
            intent.intent_id
        );
    }
    if event.imported_files.is_empty() {
        return Ok(None);
    }
    let _identity_mutation_guard = LIBRARY_IDENTITY_MUTATION_LOCK.lock().await;
    let _database_identity_guard = acquire_library_identity_database_guard(pool).await?;
    let media_type = merge_media_type_with_intent(event.media_type, intent.media_type);
    let linked = match media_type {
        MediaType::Movie => {
            ingest_managed_movie_import_files(
                pool,
                metadata,
                linkers,
                artwork,
                intent,
                event.external_ids.as_ref(),
                event.manager_implementation.clone(),
                &event.imported_files,
            )
            .await?
        }
        MediaType::Series | MediaType::Anime => {
            ingest_managed_series_import_files(
                pool,
                metadata,
                linkers,
                artwork,
                intent,
                event.external_ids.as_ref(),
                event.manager_implementation.clone(),
                media_type,
                &event.imported_files,
            )
            .await?
        }
    };

    if let Some(media_item_id) = linked {
        let store = ExtensionStore::new(pool);
        store
            .mark_managed_import_event_linked(event.event_id, media_item_id)
            .await?;
    }

    Ok(linked)
}

pub async fn ingest_managed_movie_import(
    pool: &AnyPool,
    metadata: Option<&MetadataService>,
    artwork: Option<&ArtworkService>,
    intent: &ManagedIngestIntent,
    file_path: &str,
) -> Result<Option<Uuid>> {
    if intent.media_type != MediaType::Movie {
        return Ok(None);
    }
    let _identity_mutation_guard = LIBRARY_IDENTITY_MUTATION_LOCK.lock().await;
    let _database_identity_guard = acquire_library_identity_database_guard(pool).await?;
    let file = ManagedImportFile {
        path: file_path.to_string(),
        season_number: None,
        episode_number: None,
        absolute_episode_number: None,
        episode_title: None,
        size_bytes: None,
        container: None,
        video_codec: None,
        audio_codec: None,
    };
    ingest_managed_movie_import_files(pool, metadata, None, artwork, intent, None, None, &[file])
        .await
}

#[derive(Debug, Clone)]
pub struct AcquisitionLibraryImport {
    pub media_type: MediaType,
    pub title: String,
    pub year: Option<i32>,
    pub external_ids: ExternalIds,
    pub authority: Option<AcquisitionLibraryImportAuthority>,
    pub files: Vec<AcquisitionLibraryImportFile>,
}

#[derive(Debug, Clone)]
pub struct AcquisitionLibraryImportAuthority {
    pub subscription_id: Uuid,
    pub source_provider_id: Option<Uuid>,
    pub source_extension_id: Option<String>,
}

async fn publish_acquisition_library_authority(
    pool: &AnyPool,
    media_item_id: Uuid,
    authority: Option<&AcquisitionLibraryImportAuthority>,
) -> Result<()> {
    let Some(authority) = authority else {
        return Ok(());
    };
    ExtensionStore::new(pool)
        .upsert_acquisition_media_ownership(
            media_item_id,
            authority.subscription_id,
            authority.source_provider_id,
            authority.source_extension_id.as_deref(),
        )
        .await
}

#[derive(Debug, Clone)]
pub struct AcquisitionLibraryImportFile {
    pub path: String,
    pub size_bytes: Option<i64>,
    pub season_number: Option<i32>,
    pub episode_number: Option<i32>,
    pub absolute_episode_number: Option<i32>,
    pub episode_title: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquisitionLibraryImportFileResult {
    pub path: String,
    pub media_file_id: Uuid,
    pub movie_id: Option<Uuid>,
    pub episode_id: Option<Uuid>,
    pub season_number: Option<i32>,
    pub episode_number: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquisitionLibraryImportResult {
    pub media_item_id: Uuid,
    pub files: Vec<AcquisitionLibraryImportFileResult>,
}

#[derive(Debug, Clone)]
pub struct AcquisitionLibraryTargetScaffold {
    pub media_type: MediaType,
    pub title: String,
    pub season_number: Option<i32>,
    pub episode_number: Option<i32>,
    pub absolute_episode_number: Option<i32>,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SeriesEpisodeCatalogEnsureResult {
    pub scaffolded_targets: usize,
    pub subscription_ids: Vec<Uuid>,
}

#[derive(Debug, Clone)]
struct LocalSeriesCatalogIdentity {
    title: String,
    year: Option<i32>,
    media_type: MediaType,
    external_ids: ExternalIds,
    metadata: Option<serde_json::Value>,
}

pub async fn ensure_series_episode_catalog_from_local_metadata(
    pool: &AnyPool,
    artwork: Option<&ArtworkService>,
    media_item_id: Uuid,
) -> Result<SeriesEpisodeCatalogEnsureResult> {
    let Some(series) = load_local_series_catalog_identity(pool, media_item_id).await? else {
        return Ok(SeriesEpisodeCatalogEnsureResult::default());
    };
    if !matches!(series.media_type, MediaType::Series | MediaType::Anime) {
        return Ok(SeriesEpisodeCatalogEnsureResult::default());
    }

    let mut scaffolds_by_slot: HashMap<(i32, i32), AcquisitionLibraryTargetScaffold> =
        HashMap::new();
    collect_series_metadata_video_scaffolds(&series, &mut scaffolds_by_slot);

    let mut matched_subscription_ids = HashSet::new();
    for row in load_local_acquisition_target_scaffold_rows(pool, &series).await? {
        let Some(subscription_id) = row.subscription_id else {
            continue;
        };
        if !local_acquisition_target_matches_series(&series, &row) {
            continue;
        }
        let Some(scaffold) = row.into_scaffold() else {
            continue;
        };
        let Some(season_number) = scaffold.season_number else {
            continue;
        };
        let Some(episode_number) = scaffold.episode_number else {
            continue;
        };
        matched_subscription_ids.insert(subscription_id);
        merge_local_episode_scaffold(
            &mut scaffolds_by_slot,
            (season_number, episode_number),
            scaffold,
        );
    }

    let mut scaffolds = scaffolds_by_slot.into_values().collect::<Vec<_>>();
    scaffolds.sort_by_key(|target| {
        (
            target.season_number.unwrap_or_default(),
            target.episode_number.unwrap_or_default(),
            target.title.clone(),
        )
    });
    let scaffolded_targets =
        scaffold_acquisition_library_targets(pool, artwork, media_item_id, &scaffolds).await?;
    let mut subscription_ids = matched_subscription_ids.into_iter().collect::<Vec<_>>();
    subscription_ids.sort();

    Ok(SeriesEpisodeCatalogEnsureResult {
        scaffolded_targets,
        subscription_ids,
    })
}

pub async fn scaffold_acquisition_library_targets(
    pool: &AnyPool,
    artwork: Option<&ArtworkService>,
    media_item_id: Uuid,
    targets: &[AcquisitionLibraryTargetScaffold],
) -> Result<usize> {
    let mut season_ids: HashMap<i32, Uuid> = HashMap::new();
    let mut scaffolded = 0usize;

    for target in targets {
        if !matches!(target.media_type, MediaType::Series | MediaType::Anime) {
            continue;
        }
        let Some(season_number) = target.season_number else {
            continue;
        };
        let Some(episode_number) = target.episode_number else {
            continue;
        };
        if season_number <= 0 || episode_number <= 0 {
            continue;
        }

        let season_id = if let Some(season_id) = season_ids.get(&season_number).copied() {
            season_id
        } else {
            let season_id = upsert_season(pool, media_item_id, season_number).await?;
            season_ids.insert(season_number, season_id);
            season_id
        };
        let episode_id = upsert_episode(
            pool,
            media_item_id,
            season_id,
            season_number,
            episode_number,
            target.absolute_episode_number,
        )
        .await?;

        let episode_metadata = target
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("raw").cloned())
            .or_else(|| target.metadata.clone());
        if let Some(metadata) = episode_metadata.as_ref() {
            let title = metadata
                .get("name")
                .or_else(|| metadata.get("title"))
                .and_then(serde_json::Value::as_str)
                .or(Some(target.title.as_str()))
                .map(str::trim)
                .filter(|value| !value.is_empty());
            if local_episode_metadata_has_display_content(metadata) {
                update_episode_details(
                    pool,
                    episode_id,
                    title,
                    target_episode_runtime_seconds(metadata),
                    metadata,
                )
                .await?;
            } else if let Some(title) = title {
                update_episode_title_if_missing(pool, episode_id, title).await?;
            }

            if let (Some(artwork_service), Some(url)) = (
                artwork,
                metadata
                    .get("image")
                    .or_else(|| metadata.get("thumbnail"))
                    .or_else(|| metadata.get("still"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty()),
            ) {
                sync_episode_artwork(pool, artwork_service, episode_id, url, "acquisition").await?;
            }

            if let Some(tvdb_episode_id) = target
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("tvdbEpisodeId"))
                .or_else(|| metadata.get("tvdb_id"))
                .or_else(|| metadata.get("tvdbId"))
                .or_else(|| metadata.get("id"))
                .and_then(|value| {
                    value
                        .as_str()
                        .map(str::to_string)
                        .or_else(|| value.as_i64().map(|id| id.to_string()))
                })
                .filter(|value| !value.trim().is_empty())
            {
                insert_episode_external_id(
                    pool,
                    episode_id,
                    "tvdb_episode",
                    &tvdb_episode_id,
                    "acquisition",
                )
                .await?;
            }
        } else {
            let title = target.title.trim();
            if !title.is_empty() {
                update_episode_title_if_missing(pool, episode_id, title).await?;
            }
        }

        scaffolded += 1;
    }

    Ok(scaffolded)
}

async fn load_local_series_catalog_identity(
    pool: &AnyPool,
    media_item_id: Uuid,
) -> Result<Option<LocalSeriesCatalogIdentity>> {
    let row = sqlx::query(
        "SELECT title, year, library_type, external_imdb, external_tvdb_series, external_anilist,
                CAST(metadata_json AS TEXT) AS metadata_json
         FROM series
         WHERE id = $1
         LIMIT 1",
    )
    .bind(media_item_id.to_string())
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };
    let library_type = row.get::<String, _>("library_type");
    let tvdb_series = row.try_get::<String, _>("external_tvdb_series").ok();
    Ok(Some(LocalSeriesCatalogIdentity {
        title: row.get::<String, _>("title"),
        year: row.try_get::<i64, _>("year").ok().map(|value| value as i32),
        media_type: if library_type == "anime" {
            MediaType::Anime
        } else {
            MediaType::Series
        },
        external_ids: ExternalIds {
            imdb: row.try_get::<String, _>("external_imdb").ok(),
            tvdb: tvdb_series.clone(),
            tvdb_series,
            anilist: row.try_get::<String, _>("external_anilist").ok(),
            ..Default::default()
        },
        metadata: row
            .try_get::<String, _>("metadata_json")
            .ok()
            .and_then(|value| serde_json::from_str(&value).ok()),
    }))
}

#[derive(Debug, Clone)]
struct LocalAcquisitionTargetScaffoldRow {
    subscription_id: Option<Uuid>,
    subscription_title: String,
    subscription_year: Option<i32>,
    subscription_external_ids: ExternalIds,
    media_type: MediaType,
    title: String,
    season_number: Option<i32>,
    episode_number: Option<i32>,
    absolute_episode_number: Option<i32>,
    metadata: Option<serde_json::Value>,
}

impl LocalAcquisitionTargetScaffoldRow {
    fn into_scaffold(self) -> Option<AcquisitionLibraryTargetScaffold> {
        if self.season_number.unwrap_or_default() <= 0
            || self.episode_number.unwrap_or_default() <= 0
        {
            return None;
        }
        Some(AcquisitionLibraryTargetScaffold {
            media_type: self.media_type,
            title: self.title,
            season_number: self.season_number,
            episode_number: self.episode_number,
            absolute_episode_number: self.absolute_episode_number,
            metadata: self.metadata,
        })
    }
}

async fn load_local_acquisition_target_scaffold_rows(
    pool: &AnyPool,
    series: &LocalSeriesCatalogIdentity,
) -> Result<Vec<LocalAcquisitionTargetScaffoldRow>> {
    let imdb_like = local_external_id_like(series.external_ids.imdb.as_deref());
    let tvdb_like = local_external_id_like(
        series
            .external_ids
            .tvdb_series
            .as_deref()
            .or(series.external_ids.tvdb.as_deref()),
    );
    let anilist_like = local_external_id_like(series.external_ids.anilist.as_deref());

    let rows = sqlx::query(
        "SELECT
            CAST(s.subscription_id AS TEXT) AS subscription_id,
            s.title AS subscription_title,
            s.year AS subscription_year,
            CAST(s.external_ids_json AS TEXT) AS subscription_external_ids_json,
            t.media_type,
            t.title,
            t.season_number,
            t.episode_number,
            t.absolute_episode_number,
            CAST(t.metadata_json AS TEXT) AS metadata_json
         FROM acquisition_targets t
         JOIN acquisition_subscriptions s ON s.subscription_id = t.subscription_id
         WHERE t.media_type IN ('series', 'anime')
           AND (
                LOWER(s.title) = LOWER($1)
                OR CAST(s.external_ids_json AS TEXT) LIKE $2
                OR CAST(s.external_ids_json AS TEXT) LIKE $3
                OR CAST(s.external_ids_json AS TEXT) LIKE $4
           )",
    )
    .bind(&series.title)
    .bind(imdb_like)
    .bind(tvdb_like)
    .bind(anilist_like)
    .fetch_all(pool)
    .await?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let media_type = match row.get::<String, _>("media_type").as_str() {
            "anime" => MediaType::Anime,
            "series" => MediaType::Series,
            _ => continue,
        };
        let subscription_id = row
            .try_get::<String, _>("subscription_id")
            .ok()
            .and_then(|value| Uuid::parse_str(&value).ok());
        let subscription_external_ids = row
            .try_get::<String, _>("subscription_external_ids_json")
            .ok()
            .and_then(|value| serde_json::from_str::<ExternalIds>(&value).ok())
            .unwrap_or_default();
        out.push(LocalAcquisitionTargetScaffoldRow {
            subscription_id,
            subscription_title: row.get::<String, _>("subscription_title"),
            subscription_year: row
                .try_get::<i64, _>("subscription_year")
                .ok()
                .map(|value| value as i32),
            subscription_external_ids,
            media_type,
            title: row.get::<String, _>("title"),
            season_number: row
                .try_get::<i64, _>("season_number")
                .ok()
                .map(|value| value as i32),
            episode_number: row
                .try_get::<i64, _>("episode_number")
                .ok()
                .map(|value| value as i32),
            absolute_episode_number: row
                .try_get::<i64, _>("absolute_episode_number")
                .ok()
                .map(|value| value as i32),
            metadata: row
                .try_get::<String, _>("metadata_json")
                .ok()
                .and_then(|value| serde_json::from_str(&value).ok()),
        });
    }
    Ok(out)
}

fn collect_series_metadata_video_scaffolds(
    series: &LocalSeriesCatalogIdentity,
    scaffolds_by_slot: &mut HashMap<(i32, i32), AcquisitionLibraryTargetScaffold>,
) {
    let Some(videos) = series
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("videos"))
        .and_then(serde_json::Value::as_array)
    else {
        return;
    };

    for video in videos {
        let season_number = json_i32(video.get("season"))
            .or_else(|| json_i32(video.get("seasonNumber")))
            .or_else(|| json_i32(video.get("season_number")));
        let episode_number = json_i32(video.get("episode"))
            .or_else(|| json_i32(video.get("episodeNumber")))
            .or_else(|| json_i32(video.get("episode_number")))
            .or_else(|| json_i32(video.get("number")));
        let (Some(season_number), Some(episode_number)) = (season_number, episode_number) else {
            continue;
        };
        if season_number <= 0 || episode_number <= 0 {
            continue;
        }

        let title = video
            .get("name")
            .or_else(|| video.get("title"))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("S{season_number:02}E{episode_number:02}"));
        let mut metadata = serde_json::Map::new();
        metadata.insert(
            "source".to_string(),
            serde_json::Value::String("series_metadata".to_string()),
        );
        metadata.insert("raw".to_string(), video.clone());
        if let Some(tvdb_episode_id) = json_id_string(
            video
                .get("tvdb_id")
                .or_else(|| video.get("tvdbId"))
                .or_else(|| video.get("tvdbEpisodeId")),
        ) {
            metadata.insert(
                "tvdbEpisodeId".to_string(),
                serde_json::Value::String(tvdb_episode_id),
            );
        }

        scaffolds_by_slot
            .entry((season_number, episode_number))
            .or_insert_with(|| AcquisitionLibraryTargetScaffold {
                media_type: series.media_type,
                title,
                season_number: Some(season_number),
                episode_number: Some(episode_number),
                absolute_episode_number: json_i32(video.get("absoluteNumber"))
                    .or_else(|| json_i32(video.get("absolute_episode_number"))),
                metadata: Some(serde_json::Value::Object(metadata)),
            });
    }
}

fn merge_local_episode_scaffold(
    scaffolds_by_slot: &mut HashMap<(i32, i32), AcquisitionLibraryTargetScaffold>,
    slot: (i32, i32),
    candidate: AcquisitionLibraryTargetScaffold,
) {
    match scaffolds_by_slot.entry(slot) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(candidate);
        }
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            if local_episode_scaffold_score(&candidate) > local_episode_scaffold_score(entry.get())
            {
                entry.insert(candidate);
            }
        }
    }
}

fn local_episode_scaffold_score(scaffold: &AcquisitionLibraryTargetScaffold) -> usize {
    let mut score = 0usize;
    if scaffold.absolute_episode_number.is_some() {
        score += 4;
    }
    if !scaffold.title.trim().is_empty()
        && !scaffold.title.trim().eq_ignore_ascii_case(&format!(
            "S{:02}E{:02}",
            scaffold.season_number.unwrap_or_default(),
            scaffold.episode_number.unwrap_or_default()
        ))
    {
        score += 2;
    }

    let Some(metadata) = local_episode_scaffold_content_metadata(scaffold) else {
        return score;
    };
    if json_non_empty_string(
        metadata
            .get("overview")
            .or_else(|| metadata.get("description"))
            .or_else(|| metadata.get("summary")),
    )
    .is_some()
    {
        score += 80;
    }
    if json_non_empty_string(
        metadata
            .get("image")
            .or_else(|| metadata.get("thumbnail"))
            .or_else(|| metadata.get("still")),
    )
    .is_some()
    {
        score += 40;
    }
    if target_episode_runtime_seconds(metadata).is_some() {
        score += 8;
    }
    if json_i32(metadata.get("absoluteNumber"))
        .or_else(|| json_i32(metadata.get("absolute_episode_number")))
        .is_some()
    {
        score += 4;
    }
    score
}

fn local_episode_scaffold_content_metadata(
    scaffold: &AcquisitionLibraryTargetScaffold,
) -> Option<&serde_json::Value> {
    let metadata = scaffold.metadata.as_ref()?;
    metadata.get("raw").or(Some(metadata))
}

fn local_episode_metadata_has_display_content(metadata: &serde_json::Value) -> bool {
    json_non_empty_string(
        metadata
            .get("overview")
            .or_else(|| metadata.get("description"))
            .or_else(|| metadata.get("summary")),
    )
    .is_some()
        || json_non_empty_string(
            metadata
                .get("image")
                .or_else(|| metadata.get("thumbnail"))
                .or_else(|| metadata.get("still")),
        )
        .is_some()
        || target_episode_runtime_seconds(metadata).is_some()
        || json_i32(metadata.get("absoluteNumber"))
            .or_else(|| json_i32(metadata.get("absolute_episode_number")))
            .is_some()
}

fn json_non_empty_string(value: Option<&serde_json::Value>) -> Option<&str> {
    value
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn local_acquisition_target_matches_series(
    series: &LocalSeriesCatalogIdentity,
    row: &LocalAcquisitionTargetScaffoldRow,
) -> bool {
    if series.media_type != row.media_type {
        return false;
    }
    if local_external_ids_overlap(&series.external_ids, &row.subscription_external_ids) {
        return true;
    }
    local_title_year_match(
        &series.title,
        series.year,
        &row.subscription_title,
        row.subscription_year,
    )
}

fn local_title_year_match(
    left_title: &str,
    left_year: Option<i32>,
    right_title: &str,
    right_year: Option<i32>,
) -> bool {
    if !left_title.trim().eq_ignore_ascii_case(right_title.trim()) {
        return false;
    }
    match (left_year, right_year) {
        (Some(left), Some(right)) => left == right,
        _ => true,
    }
}

fn local_external_ids_overlap(left: &ExternalIds, right: &ExternalIds) -> bool {
    local_id_eq(left.imdb.as_deref(), right.imdb.as_deref())
        || local_id_eq(left.tvdb_series.as_deref(), right.tvdb_series.as_deref())
        || local_id_eq(left.tvdb_series.as_deref(), right.tvdb.as_deref())
        || local_id_eq(left.tvdb.as_deref(), right.tvdb_series.as_deref())
        || local_id_eq(left.tvdb.as_deref(), right.tvdb.as_deref())
        || local_id_eq(left.anilist.as_deref(), right.anilist.as_deref())
}

fn local_id_eq(left: Option<&str>, right: Option<&str>) -> bool {
    let Some(left) = left.map(str::trim).filter(|value| !value.is_empty()) else {
        return false;
    };
    let Some(right) = right.map(str::trim).filter(|value| !value.is_empty()) else {
        return false;
    };
    left.eq_ignore_ascii_case(right)
}

fn local_external_id_like(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| format!("%{value}%"))
        .unwrap_or_else(|| "__elixir_no_external_id_match__".to_string())
}

pub async fn ingest_acquisition_library_import(
    pool: &AnyPool,
    request: AcquisitionLibraryImport,
) -> Result<AcquisitionLibraryImportResult> {
    ingest_acquisition_library_import_with_metadata(pool, None, None, None, request).await
}

pub async fn ingest_acquisition_library_import_with_metadata(
    pool: &AnyPool,
    metadata: Option<&MetadataService>,
    linkers: Option<&LinkerService>,
    artwork: Option<&ArtworkService>,
    request: AcquisitionLibraryImport,
) -> Result<AcquisitionLibraryImportResult> {
    if request.files.is_empty() {
        anyhow::bail!("acquisition library import requires at least one file");
    }

    if matches!(request.media_type, MediaType::Series | MediaType::Anime) {
        for file in &request.files {
            if file.season_number.is_none() {
                anyhow::bail!(
                    "series acquisition import file '{}' is missing a season number",
                    file.path
                );
            }
            if file.episode_number.is_none() {
                anyhow::bail!(
                    "series acquisition import file '{}' is missing an episode number",
                    file.path
                );
            }
        }
    }

    let _identity_mutation_guard = LIBRARY_IDENTITY_MUTATION_LOCK.lock().await;
    let _database_identity_guard = acquire_library_identity_database_guard(pool).await?;
    let authority = request.authority.clone();

    let mut identity = MediaIdentity {
        r#type: request.media_type,
        external_ids: request.external_ids.clone(),
        title: request.title.clone(),
        year: request.year,
        season: None,
        episode: None,
    };

    let result = match request.media_type {
        MediaType::Movie => {
            let Some(file) = request
                .files
                .iter()
                .find(|file| !file.path.trim().is_empty())
            else {
                anyhow::bail!("movie acquisition import has no usable file path");
            };
            let Some(descriptor) = descriptor_from_acquisition_import_file(file).await? else {
                anyhow::bail!("movie acquisition import file is missing or not a regular file");
            };
            let mut merged_ids = request.external_ids.clone();
            let movie_hydration = fetch_movie_metadata_for_identity(
                metadata,
                linkers,
                &identity,
                "acquisition movie import",
            )
            .await;
            let meta = movie_hydration.meta;
            if let Some(meta_ids) = meta.as_ref().and_then(|value| value.external_ids.clone()) {
                merged_ids = merge_external_ids(&merged_ids, Some(meta_ids));
                identity.external_ids = merged_ids.clone();
            }

            let movie_id = upsert_movie(pool, &identity, &merged_ids, meta.as_ref()).await?;
            persist_movie_external_ids(pool, movie_id, &merged_ids, "acquisition").await?;
            upsert_legacy_media_item(pool, movie_id, &identity, &merged_ids, meta.as_ref(), false)
                .await?;
            publish_acquisition_library_authority(pool, movie_id, authority.as_ref()).await?;
            let media_file =
                upsert_media_file(pool, movie_id, None, &descriptor, None, false).await?;
            link_movie_file_authoritative(pool, movie_id, media_file.id).await?;
            if let Some(duration) = media_file.duration_seconds {
                update_movie_runtime_if_missing(pool, movie_id, duration).await?;
            }
            if let Some(artwork_service) = artwork {
                sync_movie_artwork(
                    pool,
                    artwork_service,
                    movie_id,
                    meta.as_ref(),
                    movie_hydration.tvdb_movie_meta.as_ref(),
                )
                .await?;
            }
            AcquisitionLibraryImportResult {
                media_item_id: movie_id,
                files: vec![AcquisitionLibraryImportFileResult {
                    path: descriptor.path,
                    media_file_id: media_file.id,
                    movie_id: Some(movie_id),
                    episode_id: None,
                    season_number: None,
                    episode_number: None,
                }],
            }
        }
        MediaType::Series | MediaType::Anime => {
            let mut merged_ids = request.external_ids.clone();
            let meta = if let Some(service) = metadata {
                fetch_metadata_for_identity(service, &identity, "acquisition series import").await
            } else {
                None
            };
            if let Some(meta_ids) = meta.as_ref().and_then(|value| value.external_ids.clone()) {
                merged_ids = merge_external_ids(&merged_ids, Some(meta_ids));
                identity.external_ids = merged_ids.clone();
            }
            let series_ids = if request.media_type == MediaType::Anime {
                strip_anime_ids(&merged_ids)
            } else {
                merged_ids.clone()
            };
            let series_id = upsert_series(pool, &identity, &series_ids, meta.as_ref()).await?;
            upsert_legacy_media_item(
                pool,
                series_id,
                &identity,
                &series_ids,
                meta.as_ref(),
                false,
            )
            .await?;
            persist_series_external_ids(pool, series_id, &series_ids, "acquisition").await?;
            if request.media_type == MediaType::Anime {
                mark_series_as_anime(pool, series_id).await?;
            }
            publish_acquisition_library_authority(pool, series_id, authority.as_ref()).await?;

            let mut season_ids: HashMap<i32, Uuid> = HashMap::new();
            let mut media_files_by_path: HashMap<String, MediaFileUpsert> = HashMap::new();
            let mut episode_ids_by_media_file: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
            let mut results = Vec::new();

            for file in &request.files {
                let Some(season_number) = file.season_number else {
                    anyhow::bail!(
                        "series acquisition import file '{}' is missing a season number",
                        file.path
                    );
                };
                let Some(episode_number) = file.episode_number else {
                    anyhow::bail!(
                        "series acquisition import file '{}' is missing an episode number",
                        file.path
                    );
                };
                let Some(descriptor) = descriptor_from_acquisition_import_file(file).await? else {
                    anyhow::bail!(
                        "series acquisition import file '{}' is missing or not a regular file",
                        file.path
                    );
                };
                let season_id = if let Some(season_id) = season_ids.get(&season_number).copied() {
                    season_id
                } else {
                    let season_id = upsert_season(pool, series_id, season_number).await?;
                    season_ids.insert(season_number, season_id);
                    season_id
                };
                let episode_id = upsert_episode(
                    pool,
                    series_id,
                    season_id,
                    season_number,
                    episode_number,
                    file.absolute_episode_number,
                )
                .await?;
                if let Some(title) = file
                    .episode_title
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                {
                    update_episode_title_if_missing(pool, episode_id, title).await?;
                }

                let media_file = if let Some(media_file) = media_files_by_path.get(&descriptor.path)
                {
                    *media_file
                } else {
                    let media_file =
                        upsert_media_file(pool, series_id, None, &descriptor, None, false).await?;
                    media_files_by_path.insert(descriptor.path.clone(), media_file);
                    media_file
                };
                episode_ids_by_media_file
                    .entry(media_file.id)
                    .or_default()
                    .push(episode_id);
                if let Some(duration) = media_file.duration_seconds {
                    update_episode_runtime_if_missing(pool, episode_id, duration).await?;
                }
                results.push(AcquisitionLibraryImportFileResult {
                    path: descriptor.path,
                    media_file_id: media_file.id,
                    movie_id: None,
                    episode_id: Some(episode_id),
                    season_number: Some(season_number),
                    episode_number: Some(episode_number),
                });
            }

            for (media_file_id, episode_ids) in episode_ids_by_media_file {
                replace_episode_file_links_authoritative(pool, media_file_id, &episode_ids).await?;
            }

            if results.is_empty() {
                anyhow::bail!("series acquisition import did not link any files");
            }
            if let Some(artwork_service) = artwork {
                sync_series_artwork(
                    pool,
                    artwork_service,
                    series_id,
                    meta.as_ref(),
                    &series_ids,
                    request.media_type == MediaType::Anime,
                    linkers,
                    &season_ids,
                    metadata.map(|service| service.ttl_seconds()).unwrap_or(0),
                    false,
                )
                .await?;
            }
            refresh_episode_file_state(pool).await?;
            AcquisitionLibraryImportResult {
                media_item_id: series_id,
                files: results,
            }
        }
    };

    // The branch publishes acquisition ownership before creating or relinking
    // any physical file. The coordinator remains held through this return.
    Ok(result)
}

async fn descriptor_from_acquisition_import_file(
    file: &AcquisitionLibraryImportFile,
) -> Result<Option<FileDescriptor>> {
    let path = file.path.trim();
    if path.is_empty() {
        return Ok(None);
    }
    let file_metadata = match tokio::fs::metadata(path).await {
        Ok(metadata) if metadata.is_file() => metadata,
        _ => return Ok(None),
    };
    Ok(Some(FileDescriptor {
        path: path.to_string(),
        size_bytes: Some(file_metadata.len() as i64).or(file.size_bytes),
        hash: None,
        container: Path::new(path)
            .extension()
            .map(|value| value.to_string_lossy().to_string()),
        video_codec: None,
        audio_codec: None,
    }))
}

#[derive(Debug, Clone, Default)]
struct MetadataHydration {
    meta: Option<MetadataResult>,
    tvdb_movie_meta: Option<serde_json::Value>,
}

async fn fetch_metadata_for_identity(
    service: &MetadataService,
    identity: &MediaIdentity,
    context: &str,
) -> Option<MetadataResult> {
    match service.fetch_metadata(identity).await {
        Ok(metadata) => metadata,
        Err(err) => {
            tracing::warn!(
                context,
                media_type = identity.r#type.as_str(),
                title = %identity.title,
                year = ?identity.year,
                imdb = ?identity.external_ids.imdb.as_deref(),
                tmdb = ?identity.external_ids.tmdb.as_deref(),
                tvdb = ?identity.external_ids.tvdb.as_deref(),
                tvdb_series = ?identity.external_ids.tvdb_series.as_deref(),
                tvdb_movie = ?identity.external_ids.tvdb_movie.as_deref(),
                anilist = ?identity.external_ids.anilist.as_deref(),
                error = %err,
                "metadata fetch failed"
            );
            None
        }
    }
}

async fn fetch_movie_metadata_for_identity(
    metadata: Option<&MetadataService>,
    linkers: Option<&LinkerService>,
    identity: &MediaIdentity,
    context: &str,
) -> MetadataHydration {
    if let Some(linker) = linkers {
        match fetch_tvdb_movie_metadata(linker, identity).await {
            Ok(Some(hydration)) => return hydration,
            Ok(None) => {}
            Err(err) => {
                tracing::warn!(
                    context,
                    title = %identity.title,
                    year = ?identity.year,
                    imdb = ?identity.external_ids.imdb.as_deref(),
                    tvdb = ?identity.external_ids.tvdb.as_deref(),
                    tvdb_movie = ?identity.external_ids.tvdb_movie.as_deref(),
                    error = %err,
                    "tvdb movie metadata fetch failed"
                );
            }
        }
    }

    MetadataHydration {
        meta: if let Some(service) = metadata {
            fetch_metadata_for_identity(service, identity, context).await
        } else {
            None
        },
        tvdb_movie_meta: None,
    }
}

async fn fetch_tvdb_movie_metadata(
    linker: &LinkerService,
    identity: &MediaIdentity,
) -> Result<Option<MetadataHydration>> {
    if identity.r#type != MediaType::Movie {
        return Ok(None);
    }

    let mut tvdb_movie_id = identity
        .external_ids
        .tvdb_movie
        .as_ref()
        .or(identity.external_ids.tvdb.as_ref())
        .cloned();
    if tvdb_movie_id.is_none() {
        if let Some(imdb) = identity.external_ids.imdb.as_deref() {
            tvdb_movie_id = linker.link_tvdb_movie_by_imdb(imdb).await?;
        }
    }
    let Some(tvdb_movie_id) = tvdb_movie_id else {
        return Ok(None);
    };
    let Some(movie_meta) = linker.fetch_tvdb_movie(&tvdb_movie_id).await? else {
        return Ok(None);
    };
    let meta = metadata_result_from_tvdb_movie(&movie_meta, &tvdb_movie_id, &identity.external_ids);
    Ok(Some(MetadataHydration {
        meta: Some(meta),
        tvdb_movie_meta: Some(movie_meta),
    }))
}

fn metadata_result_from_tvdb_movie(
    movie_meta: &serde_json::Value,
    tvdb_movie_id: &str,
    base_ids: &ExternalIds,
) -> MetadataResult {
    let mut external_ids = ExternalIds {
        tvdb: Some(tvdb_movie_id.to_string()),
        tvdb_movie: Some(tvdb_movie_id.to_string()),
        ..Default::default()
    };
    external_ids.imdb =
        extract_tvdb_remote_id(movie_meta, &["imdb"], true).or(base_ids.imdb.clone());
    external_ids.tmdb = extract_tvdb_remote_id(movie_meta, &["tmdb", "themoviedb"], false)
        .or(base_ids.tmdb.clone());

    MetadataResult {
        metadata_json: movie_meta.clone(),
        runtime_seconds: extract_tvdb_runtime_seconds(movie_meta),
        external_ids: Some(external_ids),
        description: extract_tvdb_description(movie_meta),
        genres: Some(extract_tvdb_genres(movie_meta)).filter(|values| !values.is_empty()),
    }
}

fn extract_tvdb_runtime_seconds(meta: &serde_json::Value) -> Option<i32> {
    json_i32(meta.get("runtime")).and_then(|minutes| minutes.checked_mul(60))
}

fn json_i32(value: Option<&serde_json::Value>) -> Option<i32> {
    let value = value?;
    if let Some(number) = value.as_i64() {
        return i32::try_from(number).ok();
    }
    value.as_str()?.trim().parse::<i32>().ok()
}

fn extract_tvdb_description(meta: &serde_json::Value) -> Option<String> {
    for key in ["overview", "description", "summary"] {
        if let Some(value) = meta.get(key).and_then(serde_json::Value::as_str) {
            let value = value.trim();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    extract_tvdb_translated_description(meta)
}

fn extract_tvdb_translated_description(meta: &serde_json::Value) -> Option<String> {
    let translation_arrays = [
        meta.get("translations")
            .and_then(|translations| translations.get("overviewTranslations")),
        meta.get("overviewTranslations"),
        meta.get("overview_translations"),
    ];

    for translations in translation_arrays.into_iter().flatten() {
        let Some(entries) = translations.as_array() else {
            continue;
        };
        for entry in entries {
            if entry
                .get("isPrimary")
                .or_else(|| entry.get("is_primary"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
            {
                if let Some(value) = tvdb_translation_text(entry) {
                    return Some(value);
                }
            }
        }
        if let Some(value) = entries.iter().find_map(tvdb_translation_text) {
            return Some(value);
        }
    }

    None
}

fn tvdb_translation_text(entry: &serde_json::Value) -> Option<String> {
    entry
        .get("overview")
        .or_else(|| entry.get("description"))
        .or_else(|| entry.get("summary"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn extract_tvdb_remote_id(
    meta: &serde_json::Value,
    sources: &[&str],
    allow_imdb_prefix: bool,
) -> Option<String> {
    let values = meta
        .get("remoteIds")
        .or_else(|| meta.get("remote_ids"))
        .and_then(serde_json::Value::as_array)?;
    for entry in values {
        let Some(id) = json_id_string(entry.get("id")) else {
            continue;
        };
        if allow_imdb_prefix && id.to_ascii_lowercase().starts_with("tt") {
            return Some(id);
        }
        let source = entry
            .get("sourceName")
            .or_else(|| entry.get("source_name"))
            .or_else(|| entry.get("source"))
            .and_then(serde_json::Value::as_str)
            .map(|value| value.trim().to_ascii_lowercase())
            .unwrap_or_default();
        if sources.iter().any(|expected| source.contains(expected)) {
            return Some(id);
        }
    }
    None
}

fn json_id_string(value: Option<&serde_json::Value>) -> Option<String> {
    let value = value?;
    if let Some(text) = value.as_str() {
        let text = text.trim();
        if !text.is_empty() {
            return Some(text.to_string());
        }
    }
    if let Some(number) = value.as_i64() {
        return Some(number.to_string());
    }
    if let Some(number) = value.as_u64() {
        return Some(number.to_string());
    }
    None
}

async fn ingest_managed_movie_import_files(
    pool: &AnyPool,
    metadata: Option<&MetadataService>,
    linkers: Option<&LinkerService>,
    artwork: Option<&ArtworkService>,
    intent: &ManagedIngestIntent,
    event_external_ids: Option<&ExternalIds>,
    manager_implementation: Option<String>,
    files: &[ManagedImportFile],
) -> Result<Option<Uuid>> {
    let Some(file) = files.iter().find(|file| !file.path.trim().is_empty()) else {
        return Ok(None);
    };
    let Some(descriptor) = descriptor_from_managed_import_file(file).await? else {
        return Ok(None);
    };

    let store = ExtensionStore::new(pool);
    let mut merged_ids = intent.external_ids.clone().unwrap_or_default();
    merged_ids = merge_external_ids(&merged_ids, event_external_ids.cloned());
    let mut identity = MediaIdentity {
        r#type: MediaType::Movie,
        external_ids: merged_ids.clone(),
        title: intent.title.clone(),
        year: intent.year,
        season: None,
        episode: None,
    };

    let movie_hydration =
        fetch_movie_metadata_for_identity(metadata, linkers, &identity, "managed movie import")
            .await;
    let meta = movie_hydration.meta;
    if let Some(meta_ids) = meta.as_ref().and_then(|m| m.external_ids.clone()) {
        merged_ids = merge_external_ids(&merged_ids, Some(meta_ids));
        identity.external_ids = merged_ids.clone();
    }

    let movie_id = upsert_movie(pool, &identity, &merged_ids, meta.as_ref()).await?;
    persist_movie_external_ids(pool, movie_id, &merged_ids, "managed_import").await?;
    upsert_legacy_media_item(pool, movie_id, &identity, &merged_ids, meta.as_ref(), false).await?;

    let resolved_manager_implementation = if manager_implementation.is_some() {
        manager_implementation
    } else {
        store
            .list_providers(None)
            .await?
            .into_iter()
            .find(|provider| provider.provider_id == intent.manager_provider_id)
            .and_then(|provider| provider.implementation)
    };
    store
        .upsert_managed_library_provenance(&NewManagedLibraryProvenance {
            media_item_id: movie_id,
            media_type: MediaType::Movie,
            title: intent.title.clone(),
            normalized_title: normalize_managed_intent_title(&intent.title),
            year: intent.year,
            external_ids: Some(merged_ids.clone()),
            manager_provider_id: intent.manager_provider_id,
            manager_item_id: intent.manager_item_id.clone(),
            manager_label: intent.manager_label.clone(),
            manager_implementation: resolved_manager_implementation,
            intent_id: Some(intent.intent_id),
        })
        .await?;

    let media_file = upsert_media_file(pool, movie_id, None, &descriptor, None, false).await?;
    link_movie_file_authoritative(pool, movie_id, media_file.id).await?;
    if let Some(duration) = media_file.duration_seconds {
        update_movie_runtime_if_missing(pool, movie_id, duration).await?;
    }
    if let Some(artwork_service) = artwork {
        sync_movie_artwork(
            pool,
            artwork_service,
            movie_id,
            meta.as_ref(),
            movie_hydration.tvdb_movie_meta.as_ref(),
        )
        .await?;
    }
    store
        .mark_managed_ingest_intent_matched(intent.intent_id)
        .await?;

    Ok(Some(movie_id))
}

async fn ingest_managed_series_import_files(
    pool: &AnyPool,
    metadata: Option<&MetadataService>,
    linkers: Option<&LinkerService>,
    artwork: Option<&ArtworkService>,
    intent: &ManagedIngestIntent,
    event_external_ids: Option<&ExternalIds>,
    manager_implementation: Option<String>,
    media_type: MediaType,
    files: &[ManagedImportFile],
) -> Result<Option<Uuid>> {
    let store = ExtensionStore::new(pool);
    let mut merged_ids = intent.external_ids.clone().unwrap_or_default();
    merged_ids = merge_external_ids(&merged_ids, event_external_ids.cloned());
    let mut identity = MediaIdentity {
        r#type: media_type,
        external_ids: merged_ids.clone(),
        title: intent.title.clone(),
        year: intent.year,
        season: None,
        episode: None,
    };

    let meta = if let Some(service) = metadata {
        fetch_metadata_for_identity(service, &identity, "managed series import").await
    } else {
        None
    };
    if let Some(meta_ids) = meta.as_ref().and_then(|m| m.external_ids.clone()) {
        merged_ids = merge_external_ids(&merged_ids, Some(meta_ids));
        identity.external_ids = merged_ids.clone();
    }

    let series_ids = if media_type == MediaType::Anime {
        strip_anime_ids(&merged_ids)
    } else {
        merged_ids.clone()
    };
    let series_id = upsert_series(pool, &identity, &series_ids, meta.as_ref()).await?;
    upsert_legacy_media_item(
        pool,
        series_id,
        &identity,
        &series_ids,
        meta.as_ref(),
        false,
    )
    .await?;
    persist_series_external_ids(pool, series_id, &series_ids, "managed_import").await?;
    if media_type == MediaType::Anime {
        mark_series_as_anime(pool, series_id).await?;
    }

    let resolved_manager_implementation = if manager_implementation.is_some() {
        manager_implementation
    } else {
        store
            .list_providers(None)
            .await?
            .into_iter()
            .find(|provider| provider.provider_id == intent.manager_provider_id)
            .and_then(|provider| provider.implementation)
    };
    store
        .upsert_managed_library_provenance(&NewManagedLibraryProvenance {
            media_item_id: series_id,
            media_type,
            title: intent.title.clone(),
            normalized_title: normalize_managed_intent_title(&intent.title),
            year: intent.year,
            external_ids: Some(merged_ids.clone()),
            manager_provider_id: intent.manager_provider_id,
            manager_item_id: intent.manager_item_id.clone(),
            manager_label: intent.manager_label.clone(),
            manager_implementation: resolved_manager_implementation,
            intent_id: Some(intent.intent_id),
        })
        .await?;

    let managed_episode_tombstones = store.list_active_managed_episode_tombstones().await?;
    let mut season_ids: HashMap<i32, Uuid> = HashMap::new();
    let mut media_files_by_path: HashMap<String, MediaFileUpsert> = HashMap::new();
    let mut episode_ids_by_media_file: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    let mut linked_any = false;

    for file in files {
        let Some(season_number) = file.season_number else {
            tracing::warn!(
                intent_id = %intent.intent_id,
                path = %file.path,
                "managed series import event file is missing a season number"
            );
            continue;
        };
        let Some(episode_number) = file.episode_number else {
            tracing::warn!(
                intent_id = %intent.intent_id,
                path = %file.path,
                "managed series import event file is missing an episode number"
            );
            continue;
        };
        if match_managed_episode_tombstone(
            &identity,
            &merged_ids,
            season_number,
            episode_number,
            file.absolute_episode_number,
            &managed_episode_tombstones,
        )
        .is_some()
        {
            tracing::info!(
                intent_id = %intent.intent_id,
                path = %file.path,
                season = season_number,
                episode = episode_number,
                "skipping managed series import file because it is blocked by an episode tombstone"
            );
            continue;
        }
        let Some(descriptor) = descriptor_from_managed_import_file(file).await? else {
            continue;
        };

        let season_id = if let Some(season_id) = season_ids.get(&season_number).copied() {
            season_id
        } else {
            let season_id = upsert_season(pool, series_id, season_number).await?;
            season_ids.insert(season_number, season_id);
            season_id
        };
        let episode_id = upsert_episode(
            pool,
            series_id,
            season_id,
            season_number,
            episode_number,
            file.absolute_episode_number,
        )
        .await?;
        if let Some(title) = file
            .episode_title
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            update_episode_title_if_missing(pool, episode_id, title).await?;
        }

        let media_file = if let Some(media_file) = media_files_by_path.get(&descriptor.path) {
            media_file.clone()
        } else {
            let media_file =
                upsert_media_file(pool, series_id, None, &descriptor, None, false).await?;
            media_files_by_path.insert(descriptor.path.clone(), media_file.clone());
            media_file
        };
        episode_ids_by_media_file
            .entry(media_file.id)
            .or_default()
            .push(episode_id);
        if let Some(duration) = media_file.duration_seconds {
            update_episode_runtime_if_missing(pool, episode_id, duration).await?;
        }
    }

    for (media_file_id, episode_ids) in episode_ids_by_media_file {
        replace_episode_file_links_authoritative(pool, media_file_id, &episode_ids).await?;
        linked_any = true;
    }

    if !linked_any {
        return Ok(None);
    }

    if let Some(artwork_service) = artwork {
        sync_series_artwork(
            pool,
            artwork_service,
            series_id,
            meta.as_ref(),
            &series_ids,
            media_type == MediaType::Anime,
            linkers,
            &season_ids,
            metadata.map(|service| service.ttl_seconds()).unwrap_or(0),
            false,
        )
        .await?;
    }
    store
        .mark_managed_ingest_intent_matched(intent.intent_id)
        .await?;
    refresh_episode_file_state(pool).await?;

    Ok(Some(series_id))
}

async fn descriptor_from_managed_import_file(
    file: &ManagedImportFile,
) -> Result<Option<FileDescriptor>> {
    let path = file.path.trim();
    if path.is_empty() {
        return Ok(None);
    }
    let file_metadata = match tokio::fs::metadata(path).await {
        Ok(metadata) if metadata.is_file() => metadata,
        _ => return Ok(None),
    };
    Ok(Some(FileDescriptor {
        path: path.to_string(),
        size_bytes: Some(file_metadata.len() as i64).or(file.size_bytes),
        hash: None,
        container: file.container.clone().or_else(|| {
            Path::new(path)
                .extension()
                .map(|value| value.to_string_lossy().to_string())
        }),
        video_codec: file.video_codec.clone(),
        audio_codec: file.audio_codec.clone(),
    }))
}

pub async fn run_full_scan_with_metadata(
    pool: &AnyPool,
    metadata: Option<&MetadataService>,
    candidates: Vec<MediaFileCandidate>,
    force_metadata: bool,
    hash_dedupe: bool,
) -> Result<()> {
    run_full_scan_with_metadata_and_linkers(
        pool,
        metadata,
        None,
        None,
        None,
        candidates,
        force_metadata,
        false,
        hash_dedupe,
    )
    .await
}

pub async fn run_full_scan_with_metadata_and_linkers(
    pool: &AnyPool,
    metadata: Option<&MetadataService>,
    linkers: Option<&LinkerService>,
    classifier_config: Option<&ClassifierConfig>,
    artwork: Option<&ArtworkService>,
    candidates: Vec<MediaFileCandidate>,
    force_metadata: bool,
    force_reclassify: bool,
    hash_dedupe: bool,
) -> Result<()> {
    let classifier = build_classifier_pipeline(classifier_config);
    let anime_matching = AnimeMatchingService::disabled();
    run_full_scan_with_classifier_and_anime_matching(
        pool,
        metadata,
        linkers,
        classifier_config,
        artwork,
        &classifier,
        &anime_matching,
        candidates,
        force_metadata,
        force_reclassify,
        hash_dedupe,
    )
    .await
}

/// Production scan entry point. The matcher remains an internal optional
/// assist: deterministic classification and ani.zip mapping always run first,
/// and every model failure returns to that exact result.
pub async fn run_full_scan_with_anime_matching(
    pool: &AnyPool,
    metadata: Option<&MetadataService>,
    linkers: Option<&LinkerService>,
    classifier_config: Option<&ClassifierConfig>,
    artwork: Option<&ArtworkService>,
    anime_matching: &AnimeMatchingService,
    candidates: Vec<MediaFileCandidate>,
    force_metadata: bool,
    force_reclassify: bool,
    hash_dedupe: bool,
) -> Result<()> {
    let classifier = build_classifier_pipeline(classifier_config);
    run_full_scan_with_classifier_and_anime_matching(
        pool,
        metadata,
        linkers,
        classifier_config,
        artwork,
        &classifier,
        anime_matching,
        candidates,
        force_metadata,
        force_reclassify,
        hash_dedupe,
    )
    .await
}

async fn run_full_scan_with_classifier(
    pool: &AnyPool,
    metadata: Option<&MetadataService>,
    linkers: Option<&LinkerService>,
    classifier_config: Option<&ClassifierConfig>,
    artwork: Option<&ArtworkService>,
    classifier: &ClassifierPipeline,
    candidates: Vec<MediaFileCandidate>,
    force_metadata: bool,
    force_reclassify: bool,
    hash_dedupe: bool,
) -> Result<()> {
    let anime_matching = AnimeMatchingService::disabled();
    run_full_scan_with_classifier_and_anime_matching(
        pool,
        metadata,
        linkers,
        classifier_config,
        artwork,
        classifier,
        &anime_matching,
        candidates,
        force_metadata,
        force_reclassify,
        hash_dedupe,
    )
    .await
}

async fn run_full_scan_with_classifier_and_anime_matching(
    pool: &AnyPool,
    metadata: Option<&MetadataService>,
    linkers: Option<&LinkerService>,
    classifier_config: Option<&ClassifierConfig>,
    artwork: Option<&ArtworkService>,
    classifier: &ClassifierPipeline,
    anime_matching: &AnimeMatchingService,
    candidates: Vec<MediaFileCandidate>,
    force_metadata: bool,
    force_reclassify: bool,
    hash_dedupe: bool,
) -> Result<()> {
    let _identity_mutation_guard = LIBRARY_IDENTITY_MUTATION_LOCK.lock().await;
    let _database_identity_guard = acquire_library_identity_database_guard(pool).await?;
    let (merged, mut seen_paths): (Vec<AggregatedCandidate>, HashSet<String>) =
        merge_candidates(candidates, hash_dedupe);
    let extension_store = ExtensionStore::new(pool);
    let managed_ingest_intents = extension_store.list_active_managed_ingest_intents().await?;
    let managed_media_tombstones = extension_store
        .list_active_managed_media_tombstones()
        .await?;
    let managed_episode_tombstones = extension_store
        .list_active_managed_episode_tombstones()
        .await?;
    let provider_implementations: HashMap<Uuid, String> = extension_store
        .list_providers(None)
        .await?
        .into_iter()
        .filter_map(|provider| {
            provider
                .implementation
                .map(|implementation| (provider.provider_id, implementation))
        })
        .collect();
    let mut matched_managed_intent_ids: HashSet<Uuid> = HashSet::new();
    let anilist_bridge = build_anilist_identifier(classifier_config);
    let anilist_scorer = DefaultScorer::default();
    let hydration_ttl_seconds = metadata.map(|service| service.ttl_seconds()).unwrap_or(0);
    let mut anizip_scan_cache: HashMap<String, Option<AniZipMapping>> = HashMap::new();

    for mut candidate in merged {
        let authoritative_file_paths =
            candidate_authoritative_file_paths(pool, &candidate, hash_dedupe).await?;
        let mut merged_ids = candidate.identity.external_ids.clone();
        let mut identity_is_authoritative = false;
        if let Some(identity_lock) =
            load_managed_identity_lock_for_files(pool, &candidate.files).await?
        {
            apply_managed_identity_lock(&mut candidate.identity, &mut merged_ids, identity_lock);
            identity_is_authoritative = true;
        } else if let Some(identity_lock) =
            load_verified_acquisition_identity_lock_for_files(pool, &candidate.files).await?
        {
            apply_managed_identity_lock(&mut candidate.identity, &mut merged_ids, identity_lock);
            identity_is_authoritative = true;
        }
        let mut matched_intent = match_and_merge_managed_ingest_intent(
            &mut candidate,
            &mut merged_ids,
            &managed_ingest_intents,
        );
        if matched_intent.is_none() {
            retain_files_without_completed_anime_repair(
                pool,
                &mut candidate,
                hash_dedupe,
                &authoritative_file_paths,
            )
            .await?;
            if candidate.files.is_empty() {
                continue;
            }
        }
        if let Some(tombstone) = match_managed_media_tombstone(
            &candidate.identity,
            &merged_ids,
            &managed_media_tombstones,
        ) {
            tracing::info!(
                title = %candidate.identity.title,
                media_type = %candidate.identity.r#type.as_str(),
                tombstone_id = %tombstone.tombstone_id,
                "skipping managed media candidate because it is blocked by a tombstone"
            );
            for file in candidate.files {
                seen_paths.insert(file.descriptor.path);
            }
            continue;
        }
        let classifier_base_ids = merged_ids.clone();
        let (
            classified_ids,
            mut classification_outcomes,
            mut prefer_anime,
            tvdb_seeds,
            mut season_anilist_seeds,
        ) = classify_candidate_files(
            pool,
            classifier,
            &candidate,
            &classifier_base_ids,
            force_reclassify,
            identity_is_authoritative,
            false,
        )
        .await?;
        let model_anime_candidate = !identity_is_authoritative
            && library_candidate_may_be_anime(&candidate, &classification_outcomes);
        let classifier_emitted_ids = classified_ids.clone();
        merged_ids = classified_ids;
        suppress_conflicting_classifier_anilist_id(
            &classifier_base_ids,
            &mut merged_ids,
            &season_anilist_seeds,
            identity_is_authoritative,
        );
        if matched_intent.is_none() {
            matched_intent = match_and_merge_managed_ingest_intent(
                &mut candidate,
                &mut merged_ids,
                &managed_ingest_intents,
            );
        }
        let has_applied_classification = classification_outcomes
            .values()
            .any(|outcome| outcome.disposition.is_applied());
        // An unresolved anime candidate may use provisional classifier/provider evidence to
        // construct model context, but that evidence is not authoritative library state. Keep
        // the pre-ALM-8 placeholder path intact until the model returns one validated canonical
        // target. This is the state boundary that makes every model failure an exact
        // deterministic fallback rather than a partially-applied identity mutation.
        let requires_model_promotion = matches!(
            candidate.identity.r#type,
            MediaType::Series | MediaType::Anime
        ) && !identity_is_authoritative
            && matched_intent.is_none()
            && !has_applied_classification
            && model_anime_candidate;
        if matches!(
            candidate.identity.r#type,
            MediaType::Series | MediaType::Anime
        ) && !identity_is_authoritative
            && matched_intent.is_none()
            && !has_applied_classification
            && !model_anime_candidate
        {
            let series_id = upsert_unresolved_series_stub(pool, &candidate.identity).await?;
            for file in candidate.files {
                seen_paths.insert(file.descriptor.path.clone());
                let media_file = upsert_media_file(
                    pool,
                    series_id,
                    file.source_config_id,
                    &file.descriptor,
                    Some(&file.extension_metadata),
                    hash_dedupe,
                )
                .await?;
                let outcome = classification_outcomes
                    .get(&file.descriptor.path)
                    .cloned()
                    .unwrap_or(ClassificationOutcome {
                        disposition: ClassificationDisposition::Unresolved,
                        confidence: None,
                        hint_json: None,
                        candidates_json: None,
                        season_scope: file.season,
                        retry_supersedes_applied: false,
                        bridge_protected: false,
                        parsed_hint: None,
                        accepted_numbers: None,
                        preserve_authoritative_episode_links: false,
                        applied_identity_rows: Default::default(),
                    });
                persist_classification_outcome(pool, media_file.id, &outcome).await?;
            }
            continue;
        }

        let mut bridge_result = AnimeBridgeResult::default();
        if let Some(linker) = linkers {
            if matches!(
                candidate.identity.r#type,
                MediaType::Series | MediaType::Anime
            ) {
                if merged_ids.tvdb_series.is_none() {
                    if let Some(imdb) = merged_ids.imdb.as_ref() {
                        if let Ok(Some(tvdb_id)) = linker.link_tvdb_series_by_imdb(imdb).await {
                            tracing::trace!(
                                imdb = %imdb,
                                tvdb_id = %tvdb_id,
                                "linked tvdb series id from imdb"
                            );
                            merged_ids.tvdb_series = Some(tvdb_id);
                        }
                    }
                }
            }
            if merged_ids.anilist.is_none() && !tvdb_seeds.is_empty() {
                if let Some(tvdb_id) = merged_ids
                    .tvdb_series
                    .as_ref()
                    .or(merged_ids.tvdb.as_ref())
                    .cloned()
                {
                    tracing::trace!(
                        tvdb_id = %tvdb_id,
                        tvdb_seeds = tvdb_seeds.len(),
                        "considering tvdb anime bridge"
                    );
                    let series_meta_result = linker.fetch_tvdb_series(&tvdb_id).await;
                    match &series_meta_result {
                        Err(error) => tracing::warn!(
                            tvdb_id = %tvdb_id,
                            error = %error,
                            "TVDB series metadata lookup failed before anime bridge"
                        ),
                        Ok(None) => tracing::debug!(
                            tvdb_id = %tvdb_id,
                            "TVDB series metadata unavailable before anime bridge"
                        ),
                        Ok(Some(_)) => {}
                    }
                    if candidate.identity.r#type == MediaType::Anime {
                        let failure = match &series_meta_result {
                            Ok(None) => Some("TVDB series metadata was unavailable".to_string()),
                            Err(error) => {
                                Some(format!("TVDB series metadata lookup failed: {error}"))
                            }
                            Ok(Some(_)) => None,
                        };
                        if let Some(failure) = failure {
                            bridge_result.prefer_anime = true;
                            mark_tvdb_anime_bridge_prerequisite_unresolved(
                                &mut classification_outcomes,
                                &tvdb_seeds,
                                &failure,
                            )?;
                        }
                    }
                    if let Ok(Some(series_meta)) = series_meta_result {
                        if tvdb_indicates_anime(&series_meta) {
                            tracing::trace!(
                                tvdb_id = %tvdb_id,
                                "tvdb indicates anime; running anilist bridge"
                            );
                            bridge_result.prefer_anime = true;
                            let mut seeds: Vec<TvdbBridgeSeed> =
                                tvdb_seeds.values().cloned().collect();
                            seeds.sort_by(|a, b| {
                                b.confidence
                                    .total_cmp(&a.confidence)
                                    .then_with(|| a.season_number.cmp(&b.season_number))
                            });
                            let mut season_years: HashMap<i32, i32> = HashMap::new();
                            if let Ok(seasons) = linker.fetch_tvdb_series_seasons(&tvdb_id).await {
                                for season_meta in seasons {
                                    let Some(season_number) =
                                        extract_tvdb_season_number(&season_meta)
                                    else {
                                        continue;
                                    };
                                    if let Some(year) = extract_tvdb_season_year(&season_meta) {
                                        season_years.insert(season_number, year);
                                    }
                                }
                            }

                            for seed in seeds {
                                let season_year = season_years.get(&seed.season_number).copied();
                                let seed_result = apply_tvdb_anime_bridge(
                                    &series_meta,
                                    &anilist_bridge,
                                    &anilist_scorer,
                                    &mut merged_ids,
                                    &mut classification_outcomes,
                                    &seed,
                                    season_year,
                                )
                                .await?;
                                for (season_number, seed) in seed_result.season_anilist_ids {
                                    insert_season_anilist_seed(
                                        &mut season_anilist_seeds,
                                        season_number,
                                        seed,
                                    );
                                }
                            }
                            tracing::trace!(
                                tvdb_id = %tvdb_id,
                                season_anilist_seeds = season_anilist_seeds.len(),
                                "tvdb anime bridge complete"
                            );
                        }
                    }
                }
            }
            if bridge_result.prefer_anime {
                tracing::trace!("prefer_anime enabled by tvdb bridge");
                prefer_anime = true;
            }
        }

        if matches!(
            candidate.identity.r#type,
            MediaType::Series | MediaType::Anime
        ) && !identity_is_authoritative
        {
            retain_final_applied_classifier_state(
                &classifier_base_ids,
                &classifier_emitted_ids,
                &mut merged_ids,
                &mut season_anilist_seeds,
                &classification_outcomes,
            );
            prefer_anime = candidate.identity.r#type == MediaType::Anime
                || external_ids_have_anime_identity(&classifier_base_ids)
                || final_applied_classification_prefers_anime(&classification_outcomes);
        }

        let mut expanded_chain: Vec<AniListSeasonChainEntry> = Vec::new();
        let strongest_anilist_seed = season_anilist_seeds
            .iter()
            .filter(|(_, seed)| season_anilist_seed_is_usable(seed))
            .min_by(|left, right| {
                right
                    .1
                    .confidence
                    .total_cmp(&left.1.confidence)
                    .then_with(|| left.0.cmp(right.0))
                    .then_with(|| left.1.anilist_id.trim().cmp(right.1.anilist_id.trim()))
            })
            .map(|(season, seed)| (*season, seed.clone()));
        if let Some((seed_season, seed)) = strongest_anilist_seed {
            let expanded =
                match expand_anilist_season_chain(&anilist_bridge, seed_season, &seed).await {
                    Ok(expanded) => expanded,
                    Err(error) => {
                        tracing::warn!(
                            anilist_id = %seed.anilist_id,
                            seed_season,
                            error = %error,
                            "anilist season-chain expansion failed; continuing with known identity"
                        );
                        Vec::new()
                    }
                };
            if !expanded.is_empty() {
                tracing::trace!(
                    seed_season,
                    expanded = expanded.len(),
                    "expanded anilist season chain"
                );
                expanded_chain = expanded.clone();
                apply_anilist_relation_chain_seeds(
                    &mut season_anilist_seeds,
                    &expanded,
                    &seed.causal_paths,
                );
            }
        }

        if prefer_anime
            && !identity_is_authoritative
            && candidate.identity.r#type != MediaType::Anime
        {
            candidate.identity.r#type = MediaType::Anime;
        }

        if let Some(root_id) = select_root_anilist_id(&expanded_chain, &season_anilist_seeds) {
            if merged_ids.anilist.as_deref() != Some(root_id.as_str()) {
                tracing::trace!(
                    anilist_id = %root_id,
                    "using root anilist id for series metadata"
                );
            }
            merged_ids.anilist = Some(root_id);
        }

        let mut identity_for_meta = candidate.identity.clone();
        identity_for_meta.season = None;
        identity_for_meta.episode = None;
        identity_for_meta.external_ids = merged_ids.clone();
        let mut movie_tvdb_meta = None;
        let meta = if identity_for_meta.r#type == MediaType::Movie {
            let should_refresh = if let Some(service) = metadata {
                should_refresh_metadata(
                    pool,
                    &identity_for_meta,
                    service.ttl_seconds(),
                    force_metadata,
                )
                .await?
            } else {
                force_metadata
            };
            if should_refresh {
                let hydration = fetch_movie_metadata_for_identity(
                    metadata,
                    linkers,
                    &identity_for_meta,
                    "library scan",
                )
                .await;
                movie_tvdb_meta = hydration.tvdb_movie_meta;
                hydration.meta
            } else {
                None
            }
        } else if let Some(service) = metadata {
            let should_refresh = should_refresh_metadata(
                pool,
                &identity_for_meta,
                service.ttl_seconds(),
                force_metadata,
            )
            .await?;
            if should_refresh {
                fetch_metadata_for_identity(service, &identity_for_meta, "library scan").await
            } else {
                None
            }
        } else {
            None
        };

        if let Some(meta_ids) = meta.as_ref().and_then(|m| m.external_ids.clone()) {
            merged_ids = merge_external_ids(&merged_ids, Some(meta_ids));
        }
        if matched_intent.is_none() {
            matched_intent = match_and_merge_managed_ingest_intent(
                &mut candidate,
                &mut merged_ids,
                &managed_ingest_intents,
            );
        }

        let has_anime_ids = merged_ids.anilist.is_some()
            || merged_ids.anidb.is_some()
            || merged_ids.mal.is_some()
            || merged_ids.kitsu.is_some();
        if has_anime_ids
            && !identity_is_authoritative
            && candidate.identity.r#type != MediaType::Anime
        {
            candidate.identity.r#type = MediaType::Anime;
        }

        let mut candidate_linked = false;
        match candidate.identity.r#type {
            MediaType::Movie => {
                let movie_id =
                    upsert_movie(pool, &candidate.identity, &merged_ids, meta.as_ref()).await?;
                persist_movie_external_ids(pool, movie_id, &merged_ids, "library_scan").await?;
                upsert_legacy_media_item(
                    pool,
                    movie_id,
                    &candidate.identity,
                    &merged_ids,
                    meta.as_ref(),
                    false,
                )
                .await?;
                if let Some(intent) = matched_intent.as_ref() {
                    extension_store
                        .upsert_managed_library_provenance(&NewManagedLibraryProvenance {
                            media_item_id: movie_id,
                            media_type: candidate.identity.r#type,
                            title: candidate.identity.title.clone(),
                            normalized_title: normalize_managed_intent_title(
                                &candidate.identity.title,
                            ),
                            year: candidate.identity.year,
                            external_ids: Some(merged_ids.clone()),
                            manager_provider_id: intent.manager_provider_id,
                            manager_item_id: intent.manager_item_id.clone(),
                            manager_label: intent.manager_label.clone(),
                            manager_implementation: provider_implementations
                                .get(&intent.manager_provider_id)
                                .cloned(),
                            intent_id: Some(intent.intent_id),
                        })
                        .await?;
                }
                if let Some(artwork_service) = artwork {
                    sync_movie_artwork(
                        pool,
                        artwork_service,
                        movie_id,
                        meta.as_ref(),
                        movie_tvdb_meta.as_ref(),
                    )
                    .await?;
                }
                for file in candidate.files {
                    seen_paths.insert(file.descriptor.path.clone());
                    let media_file = upsert_media_file(
                        pool,
                        movie_id,
                        file.source_config_id,
                        &file.descriptor,
                        Some(&file.extension_metadata),
                        hash_dedupe,
                    )
                    .await?;
                    let respect_completed_repair = matched_intent.is_none()
                        && !authoritative_file_paths.contains(&file.descriptor.path);
                    if !link_movie_file_inner(
                        pool,
                        movie_id,
                        media_file.id,
                        respect_completed_repair,
                    )
                    .await?
                    {
                        // A completed anime repair won the per-file identity
                        // race. Do not persist the stale movie classifier state
                        // after the transactional link fence rejects it.
                        continue;
                    }
                    if let Some(outcome) = classification_outcomes.get(&file.descriptor.path) {
                        persist_classification_outcome(pool, media_file.id, outcome).await?;
                    }
                    candidate_linked = true;
                    if let Some(duration) = media_file.duration_seconds {
                        update_movie_runtime_if_missing(pool, movie_id, duration).await?;
                    }
                }
            }
            MediaType::Series | MediaType::Anime => {
                let mut series_ids = if candidate.identity.r#type == MediaType::Anime {
                    strip_anime_ids(&merged_ids)
                } else {
                    merged_ids.clone()
                };
                let mut series_id = if requires_model_promotion {
                    upsert_unresolved_series_stub(pool, &candidate.identity).await?
                } else {
                    upsert_series(pool, &candidate.identity, &series_ids, meta.as_ref()).await?
                };
                if !requires_model_promotion {
                    upsert_legacy_media_item(
                        pool,
                        series_id,
                        &candidate.identity,
                        &series_ids,
                        meta.as_ref(),
                        !identity_is_authoritative,
                    )
                    .await?;
                    let inserted_classifier_series_rows =
                        persist_series_external_ids(pool, series_id, &series_ids, "classifier")
                            .await?
                            .into_iter()
                            .filter(|row| {
                                !external_ids_contain_persisted_series_row(
                                    &classifier_base_ids,
                                    row,
                                )
                            })
                            .collect::<Vec<_>>();
                    attribute_inserted_classification_identity_rows(
                        &mut classification_outcomes,
                        AppliedIdentityAttributionTarget::Series {
                            claimant_season: None,
                        },
                        &inserted_classifier_series_rows,
                        None,
                    );
                    for inserted in inserted_classifier_series_rows
                        .iter()
                        .filter(|row| row.provider == "anilist")
                    {
                        let causal_paths = season_anilist_seeds
                            .values()
                            .filter(|seed| {
                                seed.anilist_id
                                    .trim()
                                    .eq_ignore_ascii_case(inserted.external_id.trim())
                            })
                            .flat_map(|seed| seed.causal_paths.iter().cloned())
                            .collect::<BTreeSet<_>>();
                        if causal_paths.is_empty() {
                            continue;
                        }
                        attribute_inserted_classification_identity_rows(
                            &mut classification_outcomes,
                            AppliedIdentityAttributionTarget::Series {
                                claimant_season: None,
                            },
                            std::slice::from_ref(inserted),
                            Some(&causal_paths),
                        );
                    }
                }

                let mut season_ids =
                    load_series_season_state(pool, series_id, &mut season_anilist_seeds).await?;

                // Materialize only seasons supported by applied deterministic evidence. Absolute-only
                // files remain unlinked until a canonical mapping supplies both season and episode.
                for file in &candidate.files {
                    let evidence = episode_number_evidence(
                        file,
                        classification_outcomes.get(&file.descriptor.path),
                    );
                    let Some(season_number) = evidence.season else {
                        continue;
                    };
                    if !season_ids.contains_key(&season_number) {
                        let season_id = upsert_season(pool, series_id, season_number).await?;
                        season_ids.insert(season_number, season_id);
                    }
                }

                if !requires_model_promotion
                    && candidate.identity.r#type == MediaType::Anime
                    && !season_anilist_seeds.is_empty()
                {
                    for season_number in season_anilist_seeds
                        .iter()
                        .filter(|(_, seed)| season_anilist_seed_is_usable(seed))
                        .map(|(season_number, _)| season_number)
                    {
                        if season_ids.contains_key(season_number) {
                            continue;
                        }
                        let season_id = upsert_season(pool, series_id, *season_number).await?;
                        season_ids.insert(*season_number, season_id);
                    }
                }

                if !requires_model_promotion && candidate.identity.r#type == MediaType::Series {
                    if let (Some(linker), Some(tvdb_id)) = (
                        linkers,
                        merged_ids.tvdb_series.as_ref().or(merged_ids.tvdb.as_ref()),
                    ) {
                        let should_refresh = !series_seasons_scaffolded_recent(
                            pool,
                            series_id,
                            Some("tvdb"),
                            hydration_ttl_seconds,
                            force_metadata,
                        )
                        .await?;
                        if should_refresh {
                            if let Ok(seasons_meta) =
                                linker.fetch_tvdb_series_seasons(tvdb_id).await
                            {
                                for season_meta in &seasons_meta {
                                    let Some(season_number) =
                                        extract_tvdb_season_number(season_meta)
                                    else {
                                        continue;
                                    };
                                    if season_number < 1 {
                                        continue;
                                    }
                                    if season_ids.contains_key(&season_number) {
                                        continue;
                                    }
                                    let season_id =
                                        upsert_season(pool, series_id, season_number).await?;
                                    season_ids.insert(season_number, season_id);
                                }
                            }
                        }
                    }
                }

                if !requires_model_promotion
                    && (candidate.identity.r#type == MediaType::Anime
                        || (!identity_is_authoritative
                            && (prefer_anime
                                || merged_ids.anilist.is_some()
                                || !season_anilist_seeds.is_empty())))
                {
                    mark_series_as_anime(pool, series_id).await?;
                }

                let persisted_series_anilist = sqlx::query_scalar::<sqlx::Any, String>(
                    "SELECT COALESCE(external_anilist, '') FROM series WHERE id = $1 LIMIT 1",
                )
                .bind(series_id.to_string())
                .fetch_optional(pool)
                .await?
                .unwrap_or_default();
                if merged_ids.anilist.is_none() && !persisted_series_anilist.trim().is_empty() {
                    merged_ids.anilist = Some(persisted_series_anilist.trim().to_string());
                }

                // Mapping preparation is deliberately complete before resolution. The scan-wide
                // cache makes every normalized AniList ID a single network attempt for this scan,
                // while the durable cache remains usable during refresh outages.
                let mut validated_mapping_targets = BTreeSet::new();
                if let Some(anilist_id) = merged_ids.anilist.as_deref() {
                    let anilist_id = anilist_id.trim();
                    if !anilist_id.is_empty() {
                        validated_mapping_targets.insert(anilist_id.to_string());
                    }
                }
                let persisted_series_anilist = persisted_series_anilist.trim();
                if !persisted_series_anilist.is_empty() {
                    validated_mapping_targets.insert(persisted_series_anilist.to_string());
                }
                for seed in season_anilist_seeds.values() {
                    let anilist_id = seed.anilist_id.trim();
                    if !anilist_id.is_empty() {
                        validated_mapping_targets.insert(anilist_id.to_string());
                    }
                }
                for season in &expanded_chain {
                    let anilist_id = season.anilist_id.trim();
                    if !anilist_id.is_empty() {
                        validated_mapping_targets.insert(anilist_id.to_string());
                    }
                }
                let mut mapping_targets = validated_mapping_targets.clone();
                if model_anime_candidate {
                    mapping_targets
                        .extend(library_provisional_anilist_ids(&classification_outcomes));
                }

                let persisted_episode_number_map =
                    load_persisted_episode_number_map(pool, series_id).await?;
                let mut current_episode_number_map = CanonicalEpisodeNumberMap::new();
                let mut mappings_by_anilist_id: HashMap<String, Arc<AniZipMapping>> =
                    HashMap::new();
                for anilist_id in mapping_targets {
                    if let Some(mapping) = anizip_mapping_for_scan(
                        pool,
                        linkers,
                        &anilist_id,
                        hydration_ttl_seconds,
                        force_metadata,
                        &mut anizip_scan_cache,
                    )
                    .await?
                    {
                        if validated_mapping_targets.contains(&anilist_id) {
                            insert_anizip_episode_numbers(
                                &mut current_episode_number_map,
                                &mapping,
                            );
                        }
                        mappings_by_anilist_id.insert(anilist_id, Arc::new(mapping));
                    }
                }
                let validated_mappings_by_anilist_id = mappings_by_anilist_id
                    .iter()
                    .filter(|(anilist_id, _)| validated_mapping_targets.contains(*anilist_id))
                    .map(|(anilist_id, mapping)| (anilist_id.clone(), Arc::clone(mapping)))
                    .collect::<HashMap<_, _>>();
                let episode_number_map = merge_authoritative_anizip_numbers(
                    persisted_episode_number_map,
                    current_episode_number_map,
                );
                merged_ids =
                    merge_root_anizip_external_ids(&merged_ids, &validated_mappings_by_anilist_id);

                let mut anizip_mappings: HashMap<i32, Arc<AniZipMapping>> = HashMap::new();
                let mut anizip_context_seasons = BTreeSet::new();
                for (season_number, seed) in &season_anilist_seeds {
                    if !season_anilist_seed_is_usable(seed) {
                        continue;
                    }
                    if let Some(mapping) = validated_mappings_by_anilist_id
                        .get(seed.anilist_id.trim())
                        .filter(|mapping| {
                            anizip_mapping_contains_relation_season(mapping, *season_number)
                        })
                    {
                        anizip_mappings.insert(*season_number, Arc::clone(mapping));
                        anizip_context_seasons.insert(*season_number);
                    }
                }
                let mut inferred_mapping_candidates: BTreeMap<
                    i32,
                    Vec<(&String, &Arc<AniZipMapping>)>,
                > = BTreeMap::new();
                for (requested_anilist_id, mapping) in &validated_mappings_by_anilist_id {
                    if let Some(season_number) = infer_anizip_mapping_season(mapping) {
                        inferred_mapping_candidates
                            .entry(season_number)
                            .or_default()
                            .push((requested_anilist_id, mapping));
                    }
                }
                for (season_number, mut mapping_candidates) in inferred_mapping_candidates {
                    if anizip_mappings.contains_key(&season_number) {
                        continue;
                    }
                    mapping_candidates.sort_by(|left, right| left.0.cmp(right.0));
                    if mapping_candidates.len() != 1 {
                        tracing::warn!(
                            season_number,
                            anilist_ids = ?mapping_candidates
                                .iter()
                                .map(|(anilist_id, _)| anilist_id.as_str())
                                .collect::<Vec<_>>(),
                            "multiple ani.zip mappings claim one inferred season; skipping ambiguous hydration"
                        );
                        continue;
                    }
                    let (requested_anilist_id, mapping) = mapping_candidates[0];
                    if !requires_model_promotion && !season_ids.contains_key(&season_number) {
                        let season_id = upsert_season(pool, series_id, season_number).await?;
                        season_ids.insert(season_number, season_id);
                    }
                    anizip_mappings.insert(season_number, Arc::clone(mapping));
                    anizip_context_seasons.insert(season_number);
                    if !season_anilist_seeds.contains_key(&season_number) {
                        let mapped_anilist_id = mapping
                            .ids
                            .anilist
                            .as_deref()
                            .filter(|value| !value.trim().is_empty())
                            .unwrap_or(requested_anilist_id)
                            .to_string();
                        insert_season_anilist_seed(
                            &mut season_anilist_seeds,
                            season_number,
                            SeasonAnilistSeed {
                                anilist_id: mapped_anilist_id,
                                confidence: 0.5,
                                causal_paths: BTreeSet::new(),
                            },
                        );
                    }
                }

                let mut resolved_numbers: HashMap<String, ResolvedEpisodeNumbers> = HashMap::new();
                for file in &candidate.files {
                    let resolved = resolve_episode_numbers(
                        file,
                        classification_outcomes.get(&file.descriptor.path),
                        candidate.identity.r#type,
                        &episode_number_map,
                    );
                    if let (Some(season_number), Some(_)) = (resolved.season, resolved.episode) {
                        if !season_ids.contains_key(&season_number) {
                            let season_id = upsert_season(pool, series_id, season_number).await?;
                            season_ids.insert(season_number, season_id);
                        }
                    }
                    resolved_numbers.insert(file.descriptor.path.clone(), resolved);
                }

                let mut model_resolved_seasons = BTreeSet::new();
                if candidate.identity.r#type == MediaType::Anime || model_anime_candidate {
                    model_resolved_seasons = resolve_difficult_library_anime_files(
                        anime_matching,
                        &candidate,
                        &expanded_chain,
                        &mut season_anilist_seeds,
                        &mappings_by_anilist_id,
                        &mut merged_ids,
                        &mut resolved_numbers,
                        &mut classification_outcomes,
                    )
                    .await;

                    // Provisional mappings are model context only. Once the model validates one
                    // canonical season, promote only that selected season/mapping for identity,
                    // hydration, and scaffolding; sibling hypotheses remain read-only.
                    for season_number in model_resolved_seasons.iter().copied() {
                        let Some(seed) = season_anilist_seeds
                            .get(&season_number)
                            .filter(|seed| season_anilist_seed_is_usable(seed))
                        else {
                            continue;
                        };
                        let mapping = mappings_by_anilist_id
                            .get(seed.anilist_id.trim())
                            .or_else(|| {
                                mappings_by_anilist_id.values().find(|mapping| {
                                    mapping.ids.anilist.as_deref().is_some_and(|id| {
                                        id.trim().eq_ignore_ascii_case(seed.anilist_id.trim())
                                    })
                                })
                            })
                            .filter(|mapping| {
                                anizip_mapping_contains_relation_season(mapping, season_number)
                            });
                        if let Some(mapping) = mapping {
                            anizip_mappings.insert(season_number, Arc::clone(mapping));
                            anizip_context_seasons.insert(season_number);
                        }
                    }

                    if requires_model_promotion && model_resolved_seasons.is_empty() {
                        // The model was unavailable, failed validation, or returned no match.
                        // Persist only the same unresolved placeholder/file state the
                        // deterministic pipeline produced, plus diagnostic assist provenance.
                        // Provisional IDs, seasons, mappings, and metadata remain read-only.
                        for file in candidate.files {
                            seen_paths.insert(file.descriptor.path.clone());
                            let media_file = upsert_media_file(
                                pool,
                                series_id,
                                file.source_config_id,
                                &file.descriptor,
                                Some(&file.extension_metadata),
                                hash_dedupe,
                            )
                            .await?;
                            let outcome = classification_outcomes
                                .get(&file.descriptor.path)
                                .cloned()
                                .unwrap_or(ClassificationOutcome {
                                    disposition: ClassificationDisposition::Unresolved,
                                    confidence: None,
                                    hint_json: None,
                                    candidates_json: None,
                                    season_scope: file.season,
                                    retry_supersedes_applied: false,
                                    bridge_protected: false,
                                    parsed_hint: None,
                                    accepted_numbers: None,
                                    preserve_authoritative_episode_links: false,
                                    applied_identity_rows: Default::default(),
                                });
                            persist_classification_outcome(pool, media_file.id, &outcome).await?;
                        }
                        continue;
                    }

                    if requires_model_promotion {
                        let placeholder_series_id = series_id;
                        let promoted_series_ids = if candidate.identity.r#type == MediaType::Anime {
                            strip_anime_ids(&merged_ids)
                        } else {
                            merged_ids.clone()
                        };
                        series_id = upsert_series(
                            pool,
                            &candidate.identity,
                            &promoted_series_ids,
                            meta.as_ref(),
                        )
                        .await?;
                        upsert_legacy_media_item(
                            pool,
                            series_id,
                            &candidate.identity,
                            &promoted_series_ids,
                            meta.as_ref(),
                            !identity_is_authoritative,
                        )
                        .await?;
                        persist_series_external_ids(
                            pool,
                            series_id,
                            &promoted_series_ids,
                            "anime_match",
                        )
                        .await?;
                        series_ids = promoted_series_ids;

                        if series_id != placeholder_series_id {
                            season_ids = load_series_season_state(
                                pool,
                                series_id,
                                &mut season_anilist_seeds,
                            )
                            .await?;
                            if let Err(error) = cleanup_orphan_series_stub(
                                pool,
                                &placeholder_series_id.to_string(),
                                &series_id.to_string(),
                            )
                            .await
                            {
                                tracing::warn!(
                                    placeholder_series_id = %placeholder_series_id,
                                    canonical_series_id = %series_id,
                                    error = %error,
                                    "model resolution promoted a canonical series but placeholder cleanup failed"
                                );
                            }
                        }

                        // Only the season selected by the validated response crosses the
                        // provisional-context boundary. Other graph candidates remain read-only.
                        for season_number in model_resolved_seasons.iter().copied() {
                            if !season_anilist_seeds
                                .get(&season_number)
                                .is_some_and(season_anilist_seed_is_usable)
                            {
                                continue;
                            }
                            if season_ids.contains_key(&season_number) {
                                continue;
                            }
                            let season_id = upsert_season(pool, series_id, season_number).await?;
                            season_ids.insert(season_number, season_id);
                        }
                    }
                    if !model_resolved_seasons.is_empty() && !identity_is_authoritative {
                        candidate.identity.r#type = MediaType::Anime;
                        mark_series_as_anime(pool, series_id).await?;
                    }
                    let model_seasons = resolved_numbers
                        .values()
                        .filter_map(|resolved| resolved.episode.and(resolved.season))
                        .collect::<BTreeSet<_>>();
                    for season_number in model_seasons {
                        if season_ids.contains_key(&season_number) {
                            continue;
                        }
                        let season_id = upsert_season(pool, series_id, season_number).await?;
                        season_ids.insert(season_number, season_id);
                    }
                }

                // A root mapping may span several seasons. Once canonical resolution identifies a
                // season, associate the one unambiguous full mapping for episode scaffolding only.
                // Season identity/title/artwork still require a direct or single-season binding.
                for season_number in season_ids.keys().copied().collect::<Vec<_>>() {
                    if anizip_mappings.contains_key(&season_number) {
                        continue;
                    }
                    let mut candidates = validated_mappings_by_anilist_id
                        .values()
                        .filter(|mapping| anizip_mapping_contains_season(mapping, season_number));
                    let first = candidates.next().cloned();
                    if first.is_some() && candidates.next().is_none() {
                        anizip_mappings.insert(season_number, first.expect("mapping exists"));
                    }
                }

                let refreshed_series_ids = if candidate.identity.r#type == MediaType::Anime {
                    strip_anime_ids(&merged_ids)
                } else {
                    merged_ids.clone()
                };
                if refreshed_series_ids != series_ids {
                    apply_external_ids_to_series(pool, series_id, &refreshed_series_ids, "anizip")
                        .await?;
                    series_ids = refreshed_series_ids;
                }

                for (season_number, season_id) in &season_ids {
                    if requires_model_promotion && !model_resolved_seasons.contains(season_number) {
                        continue;
                    }
                    if let Some(seed) = season_anilist_seeds
                        .get(season_number)
                        .filter(|seed| season_anilist_seed_is_usable(seed))
                    {
                        let ids = ExternalIds {
                            anilist: Some(seed.anilist_id.clone()),
                            ..Default::default()
                        };
                        let model_seed = model_resolved_seasons.contains(season_number);
                        let source = if model_seed {
                            "anime_match"
                        } else {
                            "classifier"
                        };
                        let inserted_season_rows = apply_external_ids_to_season_recording_rows(
                            pool,
                            *season_id,
                            &ids,
                            source,
                            Some(seed.confidence),
                        )
                        .await?;
                        if source == "classifier" {
                            attribute_inserted_classification_identity_rows(
                                &mut classification_outcomes,
                                AppliedIdentityAttributionTarget::Season {
                                    season_number: *season_number,
                                },
                                &inserted_season_rows,
                                Some(&seed.causal_paths),
                            );
                        }
                        let series_source = if model_seed {
                            "anime_match"
                        } else {
                            "anilist_chain"
                        };
                        let inserted_chain_series_rows =
                            persist_series_external_ids(pool, series_id, &ids, series_source)
                                .await?;
                        if series_source == "anilist_chain" {
                            attribute_inserted_classification_identity_rows(
                                &mut classification_outcomes,
                                AppliedIdentityAttributionTarget::Series {
                                    claimant_season: Some(*season_number),
                                },
                                &inserted_chain_series_rows,
                                Some(&seed.causal_paths),
                            );
                        }
                    }
                    if let Some(mapping) = anizip_mappings
                        .get(season_number)
                        .filter(|_| anizip_context_seasons.contains(season_number))
                    {
                        apply_external_ids_to_season(
                            pool,
                            *season_id,
                            &mapping.ids,
                            "anizip",
                            None,
                        )
                        .await?;
                        hydrate_anizip_season_context(pool, *season_id, mapping, artwork).await?;
                    }
                }

                if let Some(artwork_service) = artwork {
                    sync_series_artwork(
                        pool,
                        artwork_service,
                        series_id,
                        meta.as_ref(),
                        &series_ids,
                        candidate.identity.r#type == MediaType::Anime,
                        linkers,
                        &season_ids,
                        hydration_ttl_seconds,
                        force_metadata,
                    )
                    .await?;
                }
                if let Some(intent) = matched_intent.as_ref() {
                    extension_store
                        .upsert_managed_library_provenance(&NewManagedLibraryProvenance {
                            media_item_id: series_id,
                            media_type: candidate.identity.r#type,
                            title: candidate.identity.title.clone(),
                            normalized_title: normalize_managed_intent_title(
                                &candidate.identity.title,
                            ),
                            year: candidate.identity.year,
                            external_ids: Some(merged_ids.clone()),
                            manager_provider_id: intent.manager_provider_id,
                            manager_item_id: intent.manager_item_id.clone(),
                            manager_label: intent.manager_label.clone(),
                            manager_implementation: provider_implementations
                                .get(&intent.manager_provider_id)
                                .cloned(),
                            intent_id: Some(intent.intent_id),
                        })
                        .await?;
                }

                for (season_number, season_id) in &season_ids {
                    if requires_model_promotion && !model_resolved_seasons.contains(season_number) {
                        continue;
                    }
                    if let Some(mapping) = anizip_mappings.get(season_number) {
                        ensure_anizip_season_scaffold(
                            pool,
                            series_id,
                            *season_id,
                            *season_number,
                            mapping,
                            artwork,
                            hydration_ttl_seconds,
                            force_metadata,
                        )
                        .await?;
                    } else if let (Some(linker), Some(tvdb_id)) =
                        (linkers, merged_ids.tvdb_series.as_ref())
                    {
                        ensure_tvdb_season_scaffold(
                            pool,
                            series_id,
                            *season_id,
                            tvdb_id,
                            *season_number,
                            linker,
                            artwork,
                            hydration_ttl_seconds,
                            force_metadata,
                        )
                        .await?;
                    }
                }

                for file in candidate.files {
                    let resolved = resolved_numbers
                        .get(&file.descriptor.path)
                        .copied()
                        .unwrap_or(ResolvedEpisodeNumbers {
                            season: None,
                            episode: None,
                            absolute_episode: None,
                        });
                    if let (Some(season_number), Some(episode_number)) =
                        (resolved.season, resolved.episode)
                    {
                        if let Some(tombstone) = match_managed_episode_tombstone(
                            &candidate.identity,
                            &merged_ids,
                            season_number,
                            episode_number,
                            resolved.absolute_episode,
                            &managed_episode_tombstones,
                        ) {
                            tracing::info!(
                                title = %candidate.identity.title,
                                media_type = %candidate.identity.r#type.as_str(),
                                season = season_number,
                                episode = episode_number,
                                tombstone_id = %tombstone.tombstone_id,
                                "skipping managed episode candidate because it is blocked by an episode tombstone"
                            );
                            seen_paths.insert(file.descriptor.path);
                            continue;
                        }
                    }
                    seen_paths.insert(file.descriptor.path.clone());
                    let media_file = upsert_media_file(
                        pool,
                        series_id,
                        file.source_config_id,
                        &file.descriptor,
                        Some(&file.extension_metadata),
                        hash_dedupe,
                    )
                    .await?;
                    if let Some(outcome) = classification_outcomes.get(&file.descriptor.path) {
                        if outcome.preserve_authoritative_episode_links {
                            persist_classification_outcome(pool, media_file.id, outcome).await?;
                            candidate_linked = true;
                            tracing::trace!(
                                path = %file.descriptor.path,
                                media_file_id = %media_file.id,
                                "preserved authoritative multi-episode links"
                            );
                            continue;
                        }
                    }
                    let (Some(season_number), Some(episode_number)) =
                        (resolved.season, resolved.episode)
                    else {
                        if let Some(outcome) = classification_outcomes.get(&file.descriptor.path) {
                            let mut unresolved = outcome.clone();
                            unresolved.disposition = ClassificationDisposition::Unresolved;
                            unresolved.parsed_hint = None;
                            unresolved.accepted_numbers = None;
                            // A row that cannot produce canonical numbering is
                            // retryable even when an older scan incorrectly
                            // recorded it as Applied. Verified multi-episode
                            // imports have already exited through the explicit
                            // preservation branch above.
                            unresolved.retry_supersedes_applied = true;
                            persist_classification_outcome(pool, media_file.id, &unresolved)
                                .await?;
                        }
                        tracing::debug!(
                            path = %file.descriptor.path,
                            absolute_episode = ?resolved.absolute_episode,
                            "stored unresolved series file without creating an episode link"
                        );
                        continue;
                    };
                    let season_id = if let Some(season_id) = season_ids.get(&season_number).copied()
                    {
                        season_id
                    } else {
                        upsert_season(pool, series_id, season_number).await?
                    };
                    let episode_id = upsert_episode(
                        pool,
                        series_id,
                        season_id,
                        season_number,
                        episode_number,
                        resolved.absolute_episode,
                    )
                    .await?;
                    let respect_completed_repair = matched_intent.is_none()
                        && !authoritative_file_paths.contains(&file.descriptor.path);
                    replace_episode_file_links_inner(
                        pool,
                        media_file.id,
                        &[episode_id],
                        classification_outcomes.get(&file.descriptor.path),
                        respect_completed_repair,
                    )
                    .await?;
                    candidate_linked = true;
                    if let Some(duration) = media_file.duration_seconds {
                        update_episode_runtime_if_missing(pool, episode_id, duration).await?;
                    }
                }
            }
        }
        if candidate_linked {
            if let Some(intent) = matched_intent.as_ref() {
                matched_managed_intent_ids.insert(intent.intent_id);
            }
        }
    }

    for intent_id in matched_managed_intent_ids {
        extension_store
            .mark_managed_ingest_intent_matched(intent_id)
            .await?;
    }

    // Mark missing. A scan source can only prove that a file is absent when the
    // stored path is gone. Acquisition imports and other managed files may live
    // outside the local scan root, so do not mark them missing just because this
    // scan did not rediscover them.
    let existing_paths: Vec<String> = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT path FROM media_files WHERE scan_state = 'ok'",
    )
    .fetch_all(pool)
    .await?;

    for path in existing_paths {
        if !seen_paths.contains(&path) && media_file_path_is_missing(&path).await {
            sqlx::query::<sqlx::Any>(
                "UPDATE media_files SET scan_state = 'missing' WHERE path = $1",
            )
            .bind(path)
            .execute(pool)
            .await?;
        }
    }

    refresh_episode_file_state(pool).await?;

    Ok(())
}

async fn media_file_path_is_missing(path: &str) -> bool {
    match tokio::fs::metadata(path).await {
        Ok(metadata) => !metadata.is_file(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => true,
        Err(err) => {
            tracing::warn!(
                path = %path,
                error = %err,
                "failed to stat media file during missing-file reconciliation"
            );
            false
        }
    }
}

struct AggregatedFile {
    descriptor: FileDescriptor,
    source_config_id: Option<Uuid>,
    extension_metadata: HashMap<String, serde_json::Value>,
    season: Option<i32>,
    episode: Option<i32>,
    absolute_episode: Option<i32>,
}

struct AggregatedCandidate {
    identity: MediaIdentity,
    files: Vec<AggregatedFile>,
}

fn merge_candidates(
    candidates: Vec<MediaFileCandidate>,
    hash_dedupe: bool,
) -> (Vec<AggregatedCandidate>, HashSet<String>) {
    let mut map: HashMap<String, AggregatedCandidate> = HashMap::new();
    let mut all_paths: HashSet<String> = HashSet::new();

    for candidate in candidates {
        let mut identity = candidate.identity.clone();
        identity.season = None;
        identity.episode = None;
        let key = make_identity_key(&identity);
        let entry = map.entry(key).or_insert_with(|| AggregatedCandidate {
            identity,
            files: Vec::new(),
        });

        if entry.identity.external_ids != candidate.identity.external_ids {
            entry.identity.external_ids = merge_external_ids(
                &entry.identity.external_ids,
                Some(candidate.identity.external_ids.clone()),
            );
        }

        for file in candidate.files {
            all_paths.insert(file.path.clone());
            let dup = entry.files.iter().any(|existing| {
                if hash_dedupe {
                    if let (Some(h1), Some(h2)) = (&existing.descriptor.hash, &file.hash) {
                        return h1 == h2;
                    }
                }
                existing.descriptor.path == file.path
            });
            if dup {
                continue;
            }
            entry.files.push(AggregatedFile {
                descriptor: file,
                source_config_id: candidate.source_config_id,
                extension_metadata: candidate.extension_metadata.clone(),
                season: candidate.identity.season,
                episode: candidate.identity.episode,
                absolute_episode: None,
            });
        }
    }

    (map.into_values().collect(), all_paths)
}

fn merge_external_ids(base: &ExternalIds, incoming: Option<ExternalIds>) -> ExternalIds {
    if let Some(incoming) = incoming {
        ExternalIds {
            imdb: base.imdb.clone().or(incoming.imdb),
            tmdb: base.tmdb.clone().or(incoming.tmdb),
            tvdb: base.tvdb.clone().or(incoming.tvdb),
            tvdb_series: base.tvdb_series.clone().or(incoming.tvdb_series),
            tvdb_movie: base.tvdb_movie.clone().or(incoming.tvdb_movie),
            anilist: base.anilist.clone().or(incoming.anilist),
            anidb: base.anidb.clone().or(incoming.anidb),
            mal: base.mal.clone().or(incoming.mal),
            kitsu: base.kitsu.clone().or(incoming.kitsu),
        }
    } else {
        base.clone()
    }
}

fn merge_media_type_with_intent(current: MediaType, intent: MediaType) -> MediaType {
    match intent {
        MediaType::Anime => MediaType::Anime,
        MediaType::Series => {
            if current == MediaType::Anime {
                MediaType::Anime
            } else {
                MediaType::Series
            }
        }
        MediaType::Movie => MediaType::Movie,
    }
}

fn match_and_merge_managed_ingest_intent(
    candidate: &mut AggregatedCandidate,
    merged_ids: &mut ExternalIds,
    intents: &[ManagedIngestIntent],
) -> Option<ManagedIngestIntent> {
    let intent = match_managed_ingest_intent(&candidate.identity, merged_ids, intents)?.clone();
    if let Some(intent_ids) = intent.external_ids.clone() {
        let current_ids = merged_ids.clone();
        *merged_ids = merge_external_ids(&current_ids, Some(intent_ids));
    }
    candidate.identity.r#type =
        merge_media_type_with_intent(candidate.identity.r#type, intent.media_type);
    if !intent.title.trim().is_empty() {
        candidate.identity.title = intent.title.clone();
    }
    if intent.year.is_some() {
        candidate.identity.year = intent.year;
    }
    candidate.identity.external_ids = merged_ids.clone();
    Some(intent)
}

async fn file_has_explicit_override(
    pool: &AnyPool,
    path: &str,
    library_types: &[&str],
) -> Result<bool> {
    for library_type in library_types {
        let Some(normalized_key) = derive_override_key(library_type, path) else {
            continue;
        };
        let exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM classifier_overrides \
             WHERE library_type = $1 AND normalized_key = $2 LIMIT 1",
        )
        .bind(library_type)
        .bind(normalized_key)
        .fetch_optional(pool)
        .await?;
        if exists.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn candidate_authoritative_file_paths(
    pool: &AnyPool,
    candidate: &AggregatedCandidate,
    hash_dedupe: bool,
) -> Result<HashSet<String>> {
    let mut authoritative = HashSet::new();
    let override_library_types: &[&str] = match candidate.identity.r#type {
        MediaType::Movie => &["movie"],
        MediaType::Series | MediaType::Anime => &["anime", "series"],
    };
    for file in &candidate.files {
        let stored_authority: Option<i64> = if hash_dedupe {
            if let Some(hash) = file
                .descriptor
                .hash
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                sqlx::query_scalar(
                    "SELECT 1 FROM media_files mf \
                     WHERE (mf.path = $1 OR mf.hash = $2) AND (\
                         EXISTS (SELECT 1 FROM managed_library_provenance mlp \
                                 WHERE mlp.media_item_id = mf.media_item_id) \
                         OR EXISTS (SELECT 1 FROM acquisition_import_file_links ail \
                                    WHERE ail.media_file_id = mf.id AND ail.state = 'imported') \
                         OR EXISTS (SELECT 1 FROM media_ownerships mo \
                                    WHERE mo.media_item_id = mf.media_item_id AND mo.active = 1 \
                                      AND mo.owner_type IN ('acquisition', 'extension'))\
                     ) LIMIT 1",
                )
                .bind(&file.descriptor.path)
                .bind(hash)
                .fetch_optional(pool)
                .await?
            } else {
                stored_file_authority_by_path(pool, &file.descriptor.path).await?
            }
        } else {
            stored_file_authority_by_path(pool, &file.descriptor.path).await?
        };
        let explicit_override =
            file_has_explicit_override(pool, &file.descriptor.path, override_library_types).await?;
        if stored_authority.is_some() || explicit_override {
            authoritative.insert(file.descriptor.path.clone());
        }
    }
    Ok(authoritative)
}

async fn stored_file_authority_by_path(pool: &AnyPool, path: &str) -> Result<Option<i64>> {
    Ok(sqlx::query_scalar(
        "SELECT 1 FROM media_files mf WHERE mf.path = $1 AND (\
             EXISTS (SELECT 1 FROM managed_library_provenance mlp \
                     WHERE mlp.media_item_id = mf.media_item_id) \
             OR EXISTS (SELECT 1 FROM acquisition_import_file_links ail \
                        WHERE ail.media_file_id = mf.id AND ail.state = 'imported') \
             OR EXISTS (SELECT 1 FROM media_ownerships mo \
                        WHERE mo.media_item_id = mf.media_item_id AND mo.active = 1 \
                          AND mo.owner_type IN ('acquisition', 'extension'))\
         ) LIMIT 1",
    )
    .bind(path)
    .fetch_optional(pool)
    .await?)
}

async fn load_managed_identity_lock_for_files(
    pool: &AnyPool,
    files: &[AggregatedFile],
) -> Result<Option<ManagedIdentityLock>> {
    for file in files {
        let media_item_id: Option<String> = sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT mf.media_item_id
             FROM media_files mf
             JOIN managed_library_provenance mlp ON mlp.media_item_id = mf.media_item_id
             WHERE mf.path = $1
             LIMIT 1",
        )
        .bind(&file.descriptor.path)
        .fetch_optional(pool)
        .await?;
        if let Some(media_item_id) = media_item_id {
            if let Some(lock) = load_managed_identity_lock(pool, &media_item_id).await? {
                return Ok(Some(lock));
            }
        }
    }
    Ok(None)
}

async fn load_verified_acquisition_identity_lock_for_files(
    pool: &AnyPool,
    files: &[AggregatedFile],
) -> Result<Option<ManagedIdentityLock>> {
    for file in files {
        let row = sqlx::query(
            "SELECT mi.type, mi.title, mi.year, mi.external_ids \
             FROM media_files mf \
             JOIN acquisition_import_file_links ail ON ail.media_file_id = mf.id \
             JOIN media_items mi ON mi.id = mf.media_item_id \
             WHERE mf.path = $1 AND ail.state = 'imported' \
             ORDER BY ail.updated_at DESC LIMIT 1",
        )
        .bind(&file.descriptor.path)
        .fetch_optional(pool)
        .await?;
        let Some(row) = row else {
            continue;
        };
        let media_type = match row.try_get::<String, _>("type")?.as_str() {
            "movie" => MediaType::Movie,
            "anime" => MediaType::Anime,
            _ => MediaType::Series,
        };
        let year = row.try_get::<i64, _>("year").ok().map(|value| value as i32);
        let external_ids = row
            .try_get::<String, _>("external_ids")
            .ok()
            .and_then(|raw| serde_json::from_str::<ExternalIds>(&raw).ok());
        return Ok(Some(ManagedIdentityLock {
            media_type,
            title: row.try_get("title")?,
            year,
            external_ids,
        }));
    }
    Ok(None)
}

fn apply_managed_identity_lock(
    identity: &mut MediaIdentity,
    merged_ids: &mut ExternalIds,
    lock: ManagedIdentityLock,
) {
    identity.r#type = lock.media_type;
    identity.title = lock.title;
    identity.year = lock.year;
    if let Some(locked_ids) = lock.external_ids {
        *merged_ids = merge_external_ids(&locked_ids, Some(merged_ids.clone()));
    }
    identity.external_ids = merged_ids.clone();
}

pub(crate) fn match_managed_ingest_intent<'a>(
    identity: &MediaIdentity,
    merged_ids: &ExternalIds,
    intents: &'a [ManagedIngestIntent],
) -> Option<&'a ManagedIngestIntent> {
    let normalized_title = normalize_managed_intent_title(&identity.title);
    let mut best: Option<(&ManagedIngestIntent, i32)> = None;

    for intent in intents {
        if !managed_intent_media_type_compatible(identity.r#type, intent.media_type) {
            continue;
        }

        let id_score = intent
            .external_ids
            .as_ref()
            .map(|ids| managed_intent_id_overlap_score(merged_ids, ids))
            .unwrap_or(0);
        let title_match =
            !normalized_title.is_empty() && normalized_title == intent.normalized_title;
        let year_score = match (intent.year, identity.year) {
            (Some(intent_year), Some(identity_year)) if intent_year == identity_year => 20,
            (Some(_), Some(_)) => {
                if id_score == 0 {
                    continue;
                }
                0
            }
            (None, _) | (_, None) => 5,
        };

        if id_score == 0 && !title_match {
            continue;
        }

        let mut score = id_score * 100;
        if title_match {
            score += 30;
        }
        score += year_score;
        if intent.external_ids.is_some() {
            score += 1;
        }

        match best {
            Some((_, best_score)) if score <= best_score => {}
            _ => best = Some((intent, score)),
        }
    }

    best.map(|(intent, _)| intent)
}

fn match_managed_media_tombstone<'a>(
    identity: &MediaIdentity,
    merged_ids: &ExternalIds,
    tombstones: &'a [ManagedMediaTombstone],
) -> Option<&'a ManagedMediaTombstone> {
    let normalized_title = normalize_managed_intent_title(&identity.title);
    let mut best: Option<(&ManagedMediaTombstone, i32)> = None;

    for tombstone in tombstones {
        if !managed_intent_media_type_compatible(identity.r#type, tombstone.media_type) {
            continue;
        }

        let id_score = tombstone
            .external_ids
            .as_ref()
            .map(|ids| managed_intent_id_overlap_score(merged_ids, ids))
            .unwrap_or(0);
        let title_match =
            !normalized_title.is_empty() && normalized_title == tombstone.normalized_title;
        let year_score = match (tombstone.year, identity.year) {
            (Some(left), Some(right)) if left == right => 20,
            (Some(_), Some(_)) => {
                if id_score == 0 {
                    continue;
                }
                0
            }
            (None, _) | (_, None) => 5,
        };

        if id_score == 0 && !title_match {
            continue;
        }

        let mut score = id_score * 100;
        if title_match {
            score += 30;
        }
        score += year_score;
        if tombstone.external_ids.is_some() {
            score += 1;
        }

        match best {
            Some((_, best_score)) if score <= best_score => {}
            _ => best = Some((tombstone, score)),
        }
    }

    best.map(|(tombstone, _)| tombstone)
}

pub(crate) fn match_managed_episode_tombstone<'a>(
    identity: &MediaIdentity,
    merged_ids: &ExternalIds,
    season_number: i32,
    episode_number: i32,
    absolute_episode_number: Option<i32>,
    tombstones: &'a [ManagedEpisodeTombstone],
) -> Option<&'a ManagedEpisodeTombstone> {
    let normalized_title = normalize_managed_intent_title(&identity.title);
    let mut best: Option<(&ManagedEpisodeTombstone, i32)> = None;

    for tombstone in tombstones {
        if !managed_intent_media_type_compatible(identity.r#type, tombstone.media_type) {
            continue;
        }
        let episode_scope_score = if tombstone.season_number == season_number
            && tombstone.episode_number == episode_number
        {
            50
        } else if let (Some(left), Some(right)) =
            (tombstone.absolute_episode_number, absolute_episode_number)
        {
            if left == right { 30 } else { continue }
        } else {
            continue;
        };

        let id_score = tombstone
            .external_ids
            .as_ref()
            .map(|ids| managed_intent_id_overlap_score(merged_ids, ids))
            .unwrap_or(0);
        let title_match =
            !normalized_title.is_empty() && normalized_title == tombstone.normalized_title;
        let year_score = match (tombstone.year, identity.year) {
            (Some(left), Some(right)) if left == right => 20,
            (Some(_), Some(_)) => {
                if id_score == 0 {
                    continue;
                }
                0
            }
            (None, _) | (_, None) => 5,
        };

        if id_score == 0 && !title_match {
            continue;
        }

        let mut score = episode_scope_score + (id_score * 100) + year_score;
        if title_match {
            score += 30;
        }
        if tombstone.external_ids.is_some() {
            score += 1;
        }
        if tombstone.absolute_episode_number.is_some()
            && tombstone.absolute_episode_number == absolute_episode_number
        {
            score += 5;
        }

        match best {
            Some((_, best_score)) if score <= best_score => {}
            _ => best = Some((tombstone, score)),
        }
    }

    best.map(|(tombstone, _)| tombstone)
}

pub(crate) fn managed_episode_tombstone_matches_series(
    media_type: MediaType,
    title: &str,
    year: Option<i32>,
    external_ids: &ExternalIds,
    tombstone: &ManagedEpisodeTombstone,
) -> bool {
    if !managed_intent_media_type_compatible(media_type, tombstone.media_type) {
        return false;
    }

    let normalized_title = normalize_managed_intent_title(title);
    let id_score = tombstone
        .external_ids
        .as_ref()
        .map(|ids| managed_intent_id_overlap_score(external_ids, ids))
        .unwrap_or(0);
    let title_match =
        !normalized_title.is_empty() && normalized_title == tombstone.normalized_title;
    let year_match = match (tombstone.year, year) {
        (Some(left), Some(right)) => left == right,
        (None, _) | (_, None) => true,
    };

    year_match && (id_score > 0 || title_match)
}

fn managed_intent_media_type_compatible(candidate: MediaType, intent: MediaType) -> bool {
    candidate == intent
        || (candidate == MediaType::Series && intent == MediaType::Anime)
        || (candidate == MediaType::Anime && intent == MediaType::Series)
}

fn managed_intent_id_overlap_score(left: &ExternalIds, right: &ExternalIds) -> i32 {
    let mut score = 0;
    if managed_intent_id_match(left.imdb.as_deref(), right.imdb.as_deref(), true) {
        score += 10;
    }
    if managed_intent_id_match(left.tmdb.as_deref(), right.tmdb.as_deref(), false) {
        score += 8;
    }
    if managed_intent_id_match(
        left.tvdb_series.as_deref(),
        right.tvdb_series.as_deref(),
        false,
    ) {
        score += 8;
    }
    if managed_intent_id_match(
        left.tvdb_movie.as_deref(),
        right.tvdb_movie.as_deref(),
        false,
    ) {
        score += 8;
    }
    if managed_intent_id_match(left.tvdb.as_deref(), right.tvdb.as_deref(), false) {
        score += 6;
    }
    if managed_intent_id_match(left.anilist.as_deref(), right.anilist.as_deref(), false) {
        score += 10;
    }
    if managed_intent_id_match(left.anidb.as_deref(), right.anidb.as_deref(), false) {
        score += 6;
    }
    if managed_intent_id_match(left.mal.as_deref(), right.mal.as_deref(), false) {
        score += 6;
    }
    if managed_intent_id_match(left.kitsu.as_deref(), right.kitsu.as_deref(), false) {
        score += 4;
    }
    score
}

fn managed_intent_id_match(
    left: Option<&str>,
    right: Option<&str>,
    case_insensitive: bool,
) -> bool {
    let Some(left) = left.map(str::trim).filter(|value| !value.is_empty()) else {
        return false;
    };
    let Some(right) = right.map(str::trim).filter(|value| !value.is_empty()) else {
        return false;
    };
    if case_insensitive {
        left.eq_ignore_ascii_case(right)
    } else {
        left == right
    }
}

pub(crate) fn normalize_managed_intent_title(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '-', '_', ':'], "")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClassificationDisposition {
    Unresolved,
    Applied,
}

impl ClassificationDisposition {
    fn as_db_value(self) -> &'static str {
        match self {
            ClassificationDisposition::Unresolved => "unresolved",
            ClassificationDisposition::Applied => "applied",
        }
    }

    fn is_applied(self) -> bool {
        self == ClassificationDisposition::Applied
    }
}

#[derive(Debug, Clone)]
struct ClassificationOutcome {
    disposition: ClassificationDisposition,
    confidence: Option<f32>,
    hint_json: Option<String>,
    candidates_json: Option<String>,
    /// File-level season used only to scope later bridge decisions. It is not
    /// accepted numbering and is never consumed without an Applied disposition.
    season_scope: Option<i32>,
    /// A later bridge stage may invalidate an earlier persisted Applied row.
    /// Ordinary unresolved rescans never set this flag.
    retry_supersedes_applied: bool,
    /// Manual overrides and other authoritative classifications cannot be
    /// replaced by a derived TVDB-to-AniList bridge decision.
    bridge_protected: bool,
    parsed_hint: Option<ClassifierHint>,
    accepted_numbers: Option<ResolvedEpisodeNumbers>,
    preserve_authoritative_episode_links: bool,
    /// Exact classifier/anilist-chain rows inserted by this particular
    /// applied result. This is populated only after persistence confirms the
    /// row was new and this file is its one unambiguous claimant.
    applied_identity_rows: AppliedClassificationIdentityRows,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct AppliedClassificationIdentityRows {
    series: BTreeSet<PersistedExternalIdentityRow>,
    seasons: BTreeSet<PersistedSeasonExternalIdentityRow>,
    episodes: BTreeSet<PersistedEpisodeExternalIdentityRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PersistedExternalIdentityRow {
    provider: String,
    external_id: String,
    source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PersistedSeasonExternalIdentityRow {
    season_number: i32,
    provider: String,
    external_id: String,
    source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PersistedEpisodeExternalIdentityRow {
    episode_id: String,
    provider: String,
    external_id: String,
    source: String,
}

#[derive(Debug, Clone, Copy)]
struct ResolvedEpisodeNumbers {
    season: Option<i32>,
    episode: Option<i32>,
    absolute_episode: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct CanonicalEpisodeNumber {
    season: i32,
    episode: i32,
    absolute_episode: i32,
}

type CanonicalEpisodeNumberMap = HashMap<i32, Vec<CanonicalEpisodeNumber>>;

#[derive(Debug, Clone)]
struct CachedAniZipMapping {
    mapping: AniZipMapping,
    is_fresh: bool,
}

#[derive(Debug, Clone)]
struct TvdbBridgeSeed {
    hint: elixir_classifier::hint::ClassificationHint,
    confidence: f32,
    season_number: i32,
}

#[derive(Debug, Clone, Default)]
struct AnimeBridgeResult {
    prefer_anime: bool,
    season_anilist_ids: HashMap<i32, SeasonAnilistSeed>,
}

#[derive(Debug, Clone)]
struct SeasonAnilistSeed {
    anilist_id: String,
    confidence: f32,
    causal_paths: BTreeSet<String>,
}

#[derive(Debug, Clone)]
struct ExistingFileClassification {
    ids: ExternalIds,
    prefer_anime: bool,
    media_file_id: String,
    accepted_numbers: Option<ResolvedEpisodeNumbers>,
    authoritative: bool,
    preserve_episode_links: bool,
}

async fn load_existing_classification_for_path(
    pool: &AnyPool,
    path: &str,
    expected_type: MediaType,
    identity_is_authoritative: bool,
) -> Result<Option<ExistingFileClassification>> {
    let media_file_id: Option<String> = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT id FROM media_files WHERE path = $1 LIMIT 1",
    )
    .bind(path)
    .fetch_optional(pool)
    .await?;
    let Some(media_file_id) = media_file_id else {
        return Ok(None);
    };

    let movie_ids: Vec<String> = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT movie_id FROM movie_files WHERE media_file_id = $1 ORDER BY movie_id",
    )
    .bind(&media_file_id)
    .fetch_all(pool)
    .await?;

    let episode_rows = sqlx::query(
        "SELECT e.series_id as series_id, e.season_id as season_id, \
                e.season_number as season_number, e.episode_number as episode_number, \
                e.absolute_episode_number as absolute_episode_number \
         FROM episode_files ef JOIN episodes e ON e.id = ef.episode_id \
         WHERE ef.media_file_id = $1 ORDER BY e.series_id, e.season_number, e.episode_number",
    )
    .bind(&media_file_id)
    .fetch_all(pool)
    .await?;

    let persisted_disposition: Option<String> = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT disposition FROM classifier_resolution_state WHERE media_file_id = $1",
    )
    .bind(&media_file_id)
    .fetch_optional(pool)
    .await?;

    let authoritative: bool = sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT 1 FROM acquisition_import_file_links \
         WHERE media_file_id = $1 AND state = 'imported' LIMIT 1",
    )
    .bind(&media_file_id)
    .fetch_optional(pool)
    .await?
    .is_some();
    let existing_link_is_authoritative = authoritative || identity_is_authoritative;

    if !existing_link_is_authoritative
        && persisted_disposition.as_deref() != Some("applied")
        && persisted_disposition.is_some()
    {
        tracing::trace!(
            media_file_id = %media_file_id,
            disposition = ?persisted_disposition,
            "classifier retrying unresolved existing file"
        );
        return Ok(None);
    }

    let episode_series_count = episode_rows
        .iter()
        .map(|row| row.get::<String, _>("series_id"))
        .collect::<HashSet<_>>()
        .len();
    let expected_link_is_valid = match expected_type {
        MediaType::Movie => movie_ids.len() == 1 && episode_rows.is_empty(),
        MediaType::Series | MediaType::Anime => {
            movie_ids.is_empty()
                && (!episode_rows.is_empty())
                && episode_series_count == 1
                && (episode_rows.len() == 1 || existing_link_is_authoritative)
        }
    };
    if !expected_link_is_valid {
        if !movie_ids.is_empty() || !episode_rows.is_empty() {
            tracing::warn!(
                media_file_id = %media_file_id,
                expected_type = ?expected_type,
                movie_links = movie_ids.len(),
                episode_links = episode_rows.len(),
                "existing media file links are ambiguous; scheduling reclassification"
            );
        }
        return Ok(None);
    }

    let mut ids = ExternalIds::default();
    let mut accepted_numbers = None;
    let mut prefer_anime = false;

    if let Some(movie_id) = movie_ids.first() {
        if let Some(row) =
            sqlx::query("SELECT external_imdb, external_tmdb FROM movies WHERE id = $1 LIMIT 1")
                .bind(movie_id)
                .fetch_optional(pool)
                .await?
        {
            ids.imdb = row.try_get::<String, _>("external_imdb").ok();
            ids.tmdb = row.try_get::<String, _>("external_tmdb").ok();
        }
    }

    if let Some(row) = episode_rows.first() {
        let series_id: String = row.get("series_id");
        let season_id: String = row.get("season_id");
        if episode_rows.len() == 1 {
            accepted_numbers = Some(ResolvedEpisodeNumbers {
                season: Some(row.try_get::<i64, _>("season_number")? as i32),
                episode: Some(row.try_get::<i64, _>("episode_number")? as i32),
                absolute_episode: row
                    .try_get::<i64, _>("absolute_episode_number")
                    .ok()
                    .map(|value| value as i32),
            });
        }

        if let Some(series_row) = sqlx::query(
            "SELECT external_imdb, external_tvdb_series, external_anilist, library_type \
             FROM series WHERE id = $1 LIMIT 1",
        )
        .bind(&series_id)
        .fetch_optional(pool)
        .await?
        {
            ids.imdb = series_row.try_get::<String, _>("external_imdb").ok();
            ids.tvdb_series = series_row.try_get::<String, _>("external_tvdb_series").ok();
            if ids.anilist.is_none() {
                ids.anilist = series_row.try_get::<String, _>("external_anilist").ok();
            }
            prefer_anime = series_row
                .try_get::<String, _>("library_type")
                .ok()
                .as_deref()
                == Some("anime");
        }

        if let Some(season_row) =
            sqlx::query("SELECT external_anilist FROM seasons WHERE id = $1 LIMIT 1")
                .bind(&season_id)
                .fetch_optional(pool)
                .await?
        {
            if ids.anilist.is_none() {
                ids.anilist = season_row.try_get::<String, _>("external_anilist").ok();
            }
        }
    }

    prefer_anime = prefer_anime
        || ids.anilist.is_some()
        || ids.anidb.is_some()
        || ids.mal.is_some()
        || ids.kitsu.is_some();

    Ok(Some(ExistingFileClassification {
        ids,
        prefer_anime,
        media_file_id,
        accepted_numbers,
        authoritative,
        preserve_episode_links: existing_link_is_authoritative && episode_rows.len() > 1,
    }))
}

#[derive(Debug, Clone)]
pub struct AniListSeasonChainEntry {
    pub season_number: i32,
    pub anilist_id: String,
    pub title: String,
    pub format: Option<String>,
    pub season_year: Option<i32>,
    pub start_year: Option<i32>,
    pub status: Option<String>,
    pub episodes: Option<i32>,
    pub next_airing_episode: Option<i32>,
    pub next_airing_at: Option<i64>,
    pub confidence: f32,
}

async fn classify_candidate_files(
    pool: &AnyPool,
    classifier: &ClassifierPipeline,
    candidate: &AggregatedCandidate,
    merged_ids: &ExternalIds,
    force_reclassify: bool,
    identity_is_authoritative: bool,
    repair_mode: bool,
) -> Result<(
    ExternalIds,
    HashMap<String, ClassificationOutcome>,
    bool,
    HashMap<i32, TvdbBridgeSeed>,
    HashMap<i32, SeasonAnilistSeed>,
)> {
    let library_type = candidate.identity.r#type;
    let library_type_key = library_type_string(library_type);
    let mut updated_ids = merged_ids.clone();
    let mut outcomes: HashMap<String, ClassificationOutcome> = HashMap::new();
    let mut override_cache: HashMap<String, Option<ExternalIds>> = HashMap::new();
    let mut prefer_anime = false;
    let mut tvdb_seeds: HashMap<i32, TvdbBridgeSeed> = HashMap::new();
    let mut anilist_seeds: HashMap<i32, SeasonAnilistSeed> = HashMap::new();

    for file in &candidate.files {
        let effective_type = if prefer_anime {
            MediaType::Anime
        } else {
            library_type
        };
        let path = &file.descriptor.path;
        tracing::trace!(
            path = %path,
            library_type = ?library_type,
            effective_type = ?effective_type,
            prefer_anime,
            "classifier starting file"
        );

        // Resolve this before existing-state reuse so a manual override stays
        // authoritative even when its previously applied row is reused.
        let override_ids =
            lookup_override_for_path(pool, library_type_key, path, &mut override_cache).await?;
        let has_manual_override = override_ids.is_some();

        let mut previous_applied = None;
        if let Some(existing) = load_existing_classification_for_path(
            pool,
            path,
            effective_type,
            identity_is_authoritative,
        )
        .await?
        {
            if existing.authoritative || identity_is_authoritative || !force_reclassify {
                updated_ids = merge_external_ids(&updated_ids, Some(existing.ids));
                if existing.prefer_anime {
                    prefer_anime = true;
                }
                tracing::trace!(
                    path = %path,
                    media_file_id = %existing.media_file_id,
                    authoritative = existing.authoritative,
                    "classifier reusing applied existing file"
                );
                outcomes.insert(
                    path.clone(),
                    ClassificationOutcome {
                        disposition: ClassificationDisposition::Applied,
                        confidence: None,
                        hint_json: None,
                        candidates_json: None,
                        season_scope: file.season.or_else(|| {
                            existing.accepted_numbers.and_then(|numbers| numbers.season)
                        }),
                        retry_supersedes_applied: false,
                        bridge_protected: has_manual_override
                            || existing.authoritative
                            || identity_is_authoritative,
                        parsed_hint: None,
                        accepted_numbers: existing.accepted_numbers,
                        preserve_authoritative_episode_links: existing.preserve_episode_links,
                        applied_identity_rows: Default::default(),
                    },
                );
                continue;
            }
            previous_applied = Some(existing);
        }

        if let Some(override_ids) = override_ids {
            tracing::trace!(
                path = %path,
                override_ids = ?override_ids,
                "classifier override applied"
            );
            updated_ids = merge_external_ids(&updated_ids, Some(override_ids));
            let before_prefer = prefer_anime;
            if updated_ids.anilist.is_some()
                || updated_ids.anidb.is_some()
                || updated_ids.mal.is_some()
                || updated_ids.kitsu.is_some()
            {
                prefer_anime = true;
            }
            if prefer_anime != before_prefer {
                tracing::trace!(
                    path = %path,
                    prefer_anime,
                    "classifier prefer_anime enabled from override ids"
                );
            }
            if effective_type == MediaType::Movie
                || (file.season.is_some() && file.episode.is_some())
            {
                outcomes.insert(
                    path.clone(),
                    ClassificationOutcome {
                        disposition: ClassificationDisposition::Applied,
                        confidence: None,
                        hint_json: None,
                        candidates_json: None,
                        season_scope: file.season,
                        retry_supersedes_applied: false,
                        bridge_protected: true,
                        parsed_hint: None,
                        accepted_numbers: None,
                        preserve_authoritative_episode_links: false,
                        applied_identity_rows: Default::default(),
                    },
                );
                continue;
            }
        }

        if has_strong_ids(effective_type, &updated_ids)
            && (effective_type == MediaType::Movie
                || (file.season.is_some() && file.episode.is_some()))
        {
            tracing::trace!(
                path = %path,
                effective_type = ?effective_type,
                ids = ?updated_ids,
                "classifier strong ids present; skipping identify"
            );
            outcomes.insert(
                path.clone(),
                ClassificationOutcome {
                    disposition: ClassificationDisposition::Applied,
                    confidence: None,
                    hint_json: None,
                    candidates_json: None,
                    season_scope: file.season,
                    retry_supersedes_applied: false,
                    bridge_protected: has_manual_override || identity_is_authoritative,
                    parsed_hint: None,
                    accepted_numbers: None,
                    preserve_authoritative_episode_links: false,
                    applied_identity_rows: Default::default(),
                },
            );
            continue;
        }

        let input = build_classifier_input(file, effective_type, &updated_ids);
        let classification = classifier.classify_file(&input).await;
        let outcome = match classification {
            Err(error) => {
                tracing::warn!(
                    path = %input.path,
                    error = %error,
                    "classifier failed; retaining file for automatic retry"
                );
                ClassificationOutcome {
                    disposition: ClassificationDisposition::Unresolved,
                    confidence: None,
                    hint_json: None,
                    candidates_json: Some(
                        serde_json::json!({
                            "classificationError": error.to_string(),
                        })
                        .to_string(),
                    ),
                    season_scope: file.season,
                    retry_supersedes_applied: false,
                    bridge_protected: has_manual_override || identity_is_authoritative,
                    parsed_hint: None,
                    accepted_numbers: None,
                    preserve_authoritative_episode_links: false,
                    applied_identity_rows: Default::default(),
                }
            }
            Ok(results) => {
                for item in &results {
                    let best_confidence = item.canonical.as_ref().map(|c| c.confidence);
                    let candidate_count = item
                        .canonical
                        .as_ref()
                        .map(|c| c.considered.len())
                        .unwrap_or(0);
                    let providers: Vec<&str> = item
                        .canonical
                        .as_ref()
                        .map(|c| {
                            let mut set = std::collections::BTreeSet::new();
                            for candidate in &c.considered {
                                set.insert(candidate.provider);
                            }
                            set.into_iter().collect()
                        })
                        .unwrap_or_default();
                    tracing::trace!(
                        path = %input.path,
                        hint_type = ?item.hint.library_type,
                        hint_title = %item.hint.title,
                        season = ?item.hint.season,
                        episode = ?item.hint.episode,
                        candidates = candidate_count,
                        providers = ?providers,
                        confidence = ?best_confidence,
                        "classifier hint results"
                    );
                }
                let selection = select_best_classification(results);
                outcome_from_classification_selection(
                    selection,
                    path,
                    file,
                    effective_type,
                    identity_is_authoritative,
                    has_manual_override || identity_is_authoritative,
                    &mut updated_ids,
                    &mut prefer_anime,
                    &mut tvdb_seeds,
                    &mut anilist_seeds,
                )?
            }
        };

        let outcome = if !outcome.disposition.is_applied() {
            if let Some(existing) = previous_applied {
                if !repair_mode {
                    updated_ids = merge_external_ids(&updated_ids, Some(existing.ids));
                    if existing.prefer_anime {
                        prefer_anime = true;
                    }
                }
                if repair_mode {
                    tracing::warn!(
                        path = %path,
                        "historical anime reclassification is unresolved; retaining database evidence while marking repair retryable"
                    );
                    ClassificationOutcome {
                        retry_supersedes_applied: true,
                        bridge_protected: has_manual_override
                            || existing.authoritative
                            || identity_is_authoritative,
                        ..outcome
                    }
                } else {
                    tracing::warn!(
                        path = %path,
                        "forced reclassification was unresolved; retaining prior applied identity"
                    );
                    ClassificationOutcome {
                        disposition: ClassificationDisposition::Applied,
                        confidence: None,
                        hint_json: None,
                        candidates_json: None,
                        season_scope: file.season.or_else(|| {
                            existing.accepted_numbers.and_then(|numbers| numbers.season)
                        }),
                        retry_supersedes_applied: false,
                        bridge_protected: has_manual_override
                            || existing.authoritative
                            || identity_is_authoritative,
                        parsed_hint: None,
                        accepted_numbers: existing.accepted_numbers,
                        preserve_authoritative_episode_links: existing.preserve_episode_links,
                        applied_identity_rows: Default::default(),
                    }
                }
            } else {
                outcome
            }
        } else {
            outcome
        };
        outcomes.insert(path.clone(), outcome);
    }

    tracing::trace!(
        library_type = ?library_type,
        prefer_anime,
        tvdb_seeds = tvdb_seeds.len(),
        anilist_seeds = anilist_seeds.len(),
        ids = ?updated_ids,
        "classifier file batch complete"
    );
    Ok((
        updated_ids,
        outcomes,
        prefer_anime,
        tvdb_seeds,
        anilist_seeds,
    ))
}

#[allow(clippy::too_many_arguments)]
fn outcome_from_classification_selection(
    selection: Option<ClassificationSelection>,
    path: &str,
    file: &AggregatedFile,
    effective_type: MediaType,
    identity_is_authoritative: bool,
    bridge_protected: bool,
    updated_ids: &mut ExternalIds,
    prefer_anime: &mut bool,
    tvdb_seeds: &mut HashMap<i32, TvdbBridgeSeed>,
    anilist_seeds: &mut HashMap<i32, SeasonAnilistSeed>,
) -> Result<ClassificationOutcome> {
    let outcome = match selection {
        Some(selection) => {
            let hint = selection.hint;
            let canonical = selection.canonical;
            let hypotheses = selection.hypotheses;
            let decision = classification_disposition(canonical.as_ref(), selection.winner_margin);
            tracing::trace!(
                path = %path,
                hint_type = ?hint.library_type,
                hint_title = %hint.title,
                chosen_provider = canonical.as_ref().map(|c| c.chosen_provider),
                confidence = canonical.as_ref().map(|c| c.confidence),
                runner_up_confidence = ?selection.runner_up_confidence,
                winner_margin = ?selection.winner_margin,
                decision = %decision.as_db_value(),
                "classifier selected hint"
            );
            let (hint_json, candidates_json) = build_classification_evidence_payloads(
                &hint,
                &hypotheses,
                selection.runner_up_confidence,
                selection.winner_margin,
            )?;
            if decision.is_applied() && !identity_is_authoritative {
                if let Some(canonical) = canonical.as_ref() {
                    if canonical.chosen_provider == "tvdb" {
                        if let Some(season_number) =
                            canonical.season.or(hint.season).or(file.season)
                        {
                            let seed = TvdbBridgeSeed {
                                hint: hint.clone(),
                                confidence: canonical.confidence,
                                season_number,
                            };
                            let replace = tvdb_seeds
                                .get(&season_number)
                                .map(|current| seed.confidence > current.confidence)
                                .unwrap_or(true);
                            if replace {
                                tvdb_seeds.insert(season_number, seed);
                                tracing::trace!(
                                    path = %path,
                                    season = season_number,
                                    confidence = canonical.confidence,
                                    "classifier stored tvdb seed"
                                );
                            }
                        }
                    }
                    if let (Some(anilist_id), Some(relation_season)) =
                        (canonical.ids.anilist.as_ref(), canonical.season)
                    {
                        insert_season_anilist_seed(
                            anilist_seeds,
                            relation_season,
                            SeasonAnilistSeed {
                                anilist_id: anilist_id.clone(),
                                confidence: canonical.confidence,
                                causal_paths: BTreeSet::from([path.to_string()]),
                            },
                        );
                        tracing::trace!(
                            path = %path,
                            season = relation_season,
                            anilist_id = %anilist_id,
                            confidence = canonical.confidence,
                            "classifier considered anilist season seed"
                        );
                    }
                    let mapped = classifier_ids_to_server(&canonical.ids, effective_type);
                    *updated_ids = merge_external_ids(updated_ids, Some(mapped));
                    let before_prefer = *prefer_anime;
                    if canonical.ids.anilist.is_some()
                        || canonical.ids.anidb.is_some()
                        || canonical.ids.mal.is_some()
                        || canonical.ids.kitsu.is_some()
                    {
                        *prefer_anime = true;
                    }
                    if *prefer_anime != before_prefer {
                        tracing::trace!(
                            path = %path,
                            prefer_anime = *prefer_anime,
                            "classifier prefer_anime enabled from applied candidate ids"
                        );
                    }
                }
            }
            let accepted_numbers = decision.is_applied().then(|| ResolvedEpisodeNumbers {
                season: canonical
                    .as_ref()
                    .and_then(|candidate| candidate.season)
                    .or(hint.season),
                episode: canonical
                    .as_ref()
                    .and_then(|candidate| candidate.episode)
                    .or(hint.episode),
                absolute_episode: canonical
                    .as_ref()
                    .and_then(|candidate| candidate.absolute_episode)
                    .or(hint.absolute_episode),
            });
            let season_scope = canonical
                .as_ref()
                .and_then(|candidate| candidate.season)
                .or(hint.season)
                .or(file.season);
            ClassificationOutcome {
                disposition: decision,
                confidence: canonical.as_ref().map(|c| c.confidence),
                hint_json,
                candidates_json,
                season_scope,
                retry_supersedes_applied: false,
                bridge_protected,
                parsed_hint: decision.is_applied().then_some(hint),
                accepted_numbers,
                preserve_authoritative_episode_links: false,
                applied_identity_rows: Default::default(),
            }
        }
        None => ClassificationOutcome {
            disposition: ClassificationDisposition::Unresolved,
            confidence: None,
            hint_json: None,
            candidates_json: None,
            season_scope: file.season,
            retry_supersedes_applied: false,
            bridge_protected,
            parsed_hint: None,
            accepted_numbers: None,
            preserve_authoritative_episode_links: false,
            applied_identity_rows: Default::default(),
        },
    };
    Ok(outcome)
}

fn resolve_episode_numbers(
    file: &AggregatedFile,
    outcome: Option<&ClassificationOutcome>,
    media_type: MediaType,
    episode_number_map: &CanonicalEpisodeNumberMap,
) -> ResolvedEpisodeNumbers {
    let mut numbers = episode_number_evidence(file, outcome);
    if matches!(media_type, MediaType::Anime)
        && (numbers.season.is_none() || numbers.episode.is_none())
    {
        if let Some(absolute_episode) = numbers.absolute_episode {
            if let Some((mapped_season, mapped_episode)) = lookup_canonical_absolute_episode(
                episode_number_map,
                numbers.season,
                numbers.episode,
                absolute_episode,
            ) {
                tracing::trace!(
                    path = %file.descriptor.path,
                    absolute_episode,
                    mapped_season,
                    mapped_episode,
                    "ani.zip absolute episode mapped"
                );
                numbers.season = Some(mapped_season);
                numbers.episode = Some(mapped_episode);
            }
        }
    }
    numbers
}

const LIBRARY_ANIME_MATCH_MAX_WANTED_TARGETS: usize = 24;

#[derive(Debug, Clone)]
struct LibraryAnimeTargetEvidence {
    target_key: String,
    season_number: Option<i32>,
    episode_number: Option<i32>,
    absolute_episode_number: Option<i32>,
}

#[derive(Debug, Clone)]
struct LibraryAnimeModelResolution {
    numbers: ResolvedEpisodeNumbers,
    context_season_number: Option<i32>,
    context_anilist_id: Option<String>,
}

async fn resolve_difficult_library_anime_files(
    matching_service: &AnimeMatchingService,
    candidate: &AggregatedCandidate,
    expanded_chain: &[AniListSeasonChainEntry],
    season_anilist_seeds: &mut HashMap<i32, SeasonAnilistSeed>,
    mappings_by_anilist_id: &HashMap<String, Arc<AniZipMapping>>,
    merged_ids: &mut ExternalIds,
    resolved_numbers: &mut HashMap<String, ResolvedEpisodeNumbers>,
    classification_outcomes: &mut HashMap<String, ClassificationOutcome>,
) -> BTreeSet<i32> {
    let season_inputs = library_anime_model_season_inputs(
        &candidate.identity.title,
        expanded_chain,
        season_anilist_seeds,
        mappings_by_anilist_id,
    );
    if season_inputs.is_empty() {
        return BTreeSet::new();
    }

    let graph_fingerprint = library_anime_graph_fingerprint(&season_inputs);
    let mut matched_seasons = BTreeSet::new();
    for file in &candidate.files {
        if classification_outcomes
            .get(&file.descriptor.path)
            .is_some_and(|outcome| outcome.preserve_authoritative_episode_links)
        {
            // Verified acquisition packs own their complete link set. The
            // single-file/single-target library model contract must never
            // collapse that authoritative coverage into one episode.
            continue;
        }
        let deterministic = resolved_numbers
            .get(&file.descriptor.path)
            .copied()
            .unwrap_or(ResolvedEpisodeNumbers {
                season: None,
                episode: None,
                absolute_episode: None,
            });
        if deterministic.season.is_some() && deterministic.episode.is_some() {
            continue;
        }

        let parse_facts = library_anime_parse_facts(
            file,
            classification_outcomes.get(&file.descriptor.path),
            &candidate.identity.title,
        );
        let wanted_targets =
            library_anime_wanted_targets(&candidate.identity.title, &season_inputs, &parse_facts);
        if wanted_targets.is_empty() {
            continue;
        }
        let wanted_target_keys = wanted_targets
            .iter()
            .map(|target| target.target_key.clone())
            .collect::<Vec<_>>();
        let target_seasons = wanted_targets
            .iter()
            .filter_map(|target| target.season_number)
            .collect::<BTreeSet<_>>();
        let season_number = (target_seasons.len() == 1)
            .then(|| target_seasons.iter().next().copied())
            .flatten();
        let episode_numbers = wanted_targets
            .iter()
            .filter_map(|target| target.episode_number)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let absolute_episode_numbers = wanted_targets
            .iter()
            .filter_map(|target| target.absolute_episode_number)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let request_hash = blake3::hash(
            format!(
                "{}\0{}\0{}",
                file.descriptor.path,
                graph_fingerprint,
                wanted_target_keys.join("\0")
            )
            .as_bytes(),
        )
        .to_hex();
        let input = LibraryAnimeMatchRequestInput {
            request_id: format!("library-{request_hash}"),
            target: AnimeMatchTarget {
                media_type: AnimeMatchMediaType::Anime,
                canonical_title: candidate.identity.title.clone(),
                scope: if wanted_target_keys.len() == 1 {
                    AnimeMatchScope::Episode
                } else {
                    AnimeMatchScope::SelectedTargets
                },
                wanted_target_keys,
                season_number,
                episode_numbers,
                absolute_episode_numbers,
                audio_preference: AnimeMatchAudioPreference::default(),
            },
            graph_fingerprint: graph_fingerprint.clone(),
            seasons: season_inputs.clone(),
            files: vec![LibraryAnimeMatchFileInput {
                path: file.descriptor.path.clone(),
                candidate_title: None,
                parse_facts,
            }],
        };
        let batch = match library_anime_match_batch_input(input) {
            Ok(batch) => batch,
            Err(error) => {
                tracing::warn!(
                    path = %file.descriptor.path,
                    error = %error,
                    "library anime matching request preparation failed; retaining deterministic result"
                );
                continue;
            }
        };
        let prepared = match AnimeMatchingService::prepare_request(batch) {
            Ok(prepared) => prepared,
            Err(error) => {
                tracing::warn!(
                    path = %file.descriptor.path,
                    error = %error,
                    "library anime semantic request validation failed"
                );
                continue;
            }
        };
        let Some(wire_candidate) = prepared.request().candidates.first() else {
            continue;
        };
        let facts = wire_candidate.parse_facts.clone();
        // The library resolver already narrowed the graph to the canonical
        // coordinates that could explain this file. Preserve those complete
        // server-authored interpretations even when an earlier parser only
        // recovered the title (for example `Root A - 01`).
        let observed_seasons = facts.season_numbers.iter().copied().chain(
            wanted_targets
                .iter()
                .filter_map(|target| target.season_number),
        );
        let observed_episodes = facts.episode_numbers.iter().copied().chain(
            wanted_targets
                .iter()
                .filter_map(|target| target.episode_number),
        );
        let observed_absolute_episodes = facts.absolute_episode_numbers.iter().copied().chain(
            wanted_targets
                .iter()
                .filter_map(|target| target.absolute_episode_number),
        );
        let semantic_request = match build_semantic_evidence_request(
            prepared.request(),
            wire_candidate.candidate_key.clone(),
            wire_candidate.title.clone(),
            None,
            facts.title_candidates.iter().cloned(),
            observed_seasons,
            observed_episodes,
            observed_absolute_episodes,
            [AnimeSemanticMediaKind::Episode],
        ) {
            Ok(Some(request)) => request,
            Ok(None) => continue,
            Err(error) => {
                tracing::warn!(path = %file.descriptor.path, error = %error, "library anime semantic hypotheses could not be built");
                continue;
            }
        };
        let canonical_request = prepared.request().clone();
        let outcome = matching_service
            .select_semantic_hypothesis(semantic_request)
            .await;
        let mut provenance = outcome.provenance.clone();
        let mut used_model = false;
        let mut resolved = LibraryAnimeModelResolution {
            numbers: deterministic,
            context_season_number: None,
            context_anilist_id: None,
        };
        if let Some(hypothesis) = outcome.hypothesis.as_ref()
            && hypothesis.target_keys.len() == 1
            && canonical_request
                .target
                .wanted_target_keys
                .contains(&hypothesis.target_keys[0])
        {
            let targets = canonical_request
                .context
                .seasons
                .iter()
                .flat_map(|season| season.targets.iter().map(move |target| (season, target)))
                .filter(|(_, target)| target.target_key == hypothesis.target_keys[0])
                .collect::<Vec<_>>();
            if targets.len() == 1 {
                let (context_season, target) = targets[0];
                if let (Some(season), Some(episode)) = (
                    target.season_number.filter(|number| *number >= 0),
                    target.episode_number.filter(|number| *number > 0),
                ) {
                    resolved = LibraryAnimeModelResolution {
                        numbers: ResolvedEpisodeNumbers {
                            season: Some(season),
                            episode: Some(episode),
                            absolute_episode: target
                                .absolute_episode_number
                                .filter(|number| *number > 0),
                        },
                        context_season_number: Some(context_season.season_number),
                        context_anilist_id: Some(context_season.anilist_id.clone()),
                    };
                    used_model = true;
                }
            }
        }
        if outcome.selected() && !used_model {
            provenance.source =
                crate::anime_matching::AnimeMatchAssistSource::DeterministicFallback;
            provenance.result = crate::anime_matching::AnimeMatchAssistResult::Fallback;
            provenance.reason =
                Some(crate::anime_matching::AnimeMatchFallbackReason::CoverageValidationFailed);
            provenance.detail = Some(
                "semantic hypothesis did not resolve one uniquely wanted canonical episode"
                    .to_string(),
            );
        }
        if let Some(classification) = classification_outcomes.get_mut(&file.descriptor.path) {
            classification.candidates_json = Some(merge_library_anime_match_provenance(
                classification.candidates_json.as_deref(),
                &provenance,
            ));
            if used_model {
                classification.disposition = ClassificationDisposition::Applied;
                classification.accepted_numbers = Some(resolved.numbers);
                classification.parsed_hint = None;
                classification.retry_supersedes_applied = true;
                classification.preserve_authoritative_episode_links = false;
            }
        }
        if used_model {
            if let (Some(context_season), Some(target_season), Some(anilist_id)) = (
                resolved.context_season_number,
                resolved.numbers.season.filter(|number| *number >= 0),
                resolved.context_anilist_id.as_deref(),
            ) {
                tracing::trace!(
                    context_season,
                    target_season,
                    anilist_id,
                    "preserving provider target numbering for model-resolved relation identity"
                );
                matched_seasons.insert(target_season);
                insert_season_anilist_seed(
                    season_anilist_seeds,
                    target_season,
                    SeasonAnilistSeed {
                        anilist_id: anilist_id.to_string(),
                        confidence: 1.0,
                        causal_paths: BTreeSet::from([file.descriptor.path.clone()]),
                    },
                );
                if let Some(mapping) = mappings_by_anilist_id.get(anilist_id).or_else(|| {
                    mappings_by_anilist_id.values().find(|mapping| {
                        mapping
                            .ids
                            .anilist
                            .as_deref()
                            .is_some_and(|id| id.trim().eq_ignore_ascii_case(anilist_id.trim()))
                    })
                }) {
                    *merged_ids = merge_external_ids(merged_ids, Some(mapping.ids.clone()));
                } else if merged_ids.anilist.is_none() {
                    merged_ids.anilist = Some(anilist_id.to_string());
                }
            }
            resolved_numbers.insert(file.descriptor.path.clone(), resolved.numbers);
        }
    }
    matched_seasons
}

fn library_anime_model_season_inputs(
    canonical_title: &str,
    expanded_chain: &[AniListSeasonChainEntry],
    season_anilist_seeds: &HashMap<i32, SeasonAnilistSeed>,
    mappings_by_anilist_id: &HashMap<String, Arc<AniZipMapping>>,
) -> Vec<LibraryAnimeMatchSeasonInput> {
    // Keep relation season in the key. Collapsing solely by AniList ID hides
    // contradictory graph assignments before the adapter can reject them.
    let mut entries = BTreeMap::<(i32, String), AniListSeasonChainEntry>::new();
    let mut relation_ids = HashSet::new();
    for entry in expanded_chain {
        let id = entry.anilist_id.trim();
        if !id.is_empty() {
            relation_ids.insert(id.to_ascii_lowercase());
            entries
                .entry((entry.season_number, id.to_ascii_lowercase()))
                .or_insert_with(|| entry.clone());
        }
    }
    for (season_number, seed) in season_anilist_seeds {
        let id = seed.anilist_id.trim();
        if id.is_empty() {
            continue;
        }
        if relation_ids.contains(&id.to_ascii_lowercase()) {
            // A direct ani.zip/TVDB seed is keyed by target numbering. The
            // same AniList work already has its independent relation ordinal,
            // so adding it again would conflate the two numbering systems.
            continue;
        }
        entries
            .entry((*season_number, id.to_ascii_lowercase()))
            .or_insert_with(|| AniListSeasonChainEntry {
                season_number: *season_number,
                anilist_id: id.to_string(),
                title: canonical_title.to_string(),
                format: None,
                season_year: None,
                start_year: None,
                status: None,
                episodes: None,
                next_airing_episode: None,
                next_airing_at: None,
                confidence: seed.confidence,
            });
    }
    for (requested_id, mapping) in mappings_by_anilist_id {
        let mapped_id = mapping
            .ids
            .anilist
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| requested_id.trim());
        if mapped_id.is_empty() {
            continue;
        }
        if relation_ids.contains(&mapped_id.to_ascii_lowercase()) {
            continue;
        }
        let season_number = infer_anizip_mapping_season(mapping)
            .or_else(|| {
                mapping
                    .episodes
                    .iter()
                    .filter_map(|episode| episode.season_number)
                    .min()
            })
            .unwrap_or(1);
        let title = preferred_anizip_title(mapping)
            .unwrap_or(canonical_title)
            .to_string();
        entries
            .entry((season_number, mapped_id.to_ascii_lowercase()))
            .or_insert_with(|| AniListSeasonChainEntry {
                season_number,
                anilist_id: mapped_id.to_string(),
                title,
                format: None,
                season_year: None,
                start_year: None,
                status: None,
                episodes: None,
                next_airing_episode: None,
                next_airing_at: None,
                confidence: 0.5,
            });
    }

    let mut inputs = Vec::new();
    for season in entries.into_values() {
        let relation_identity = relation_ids.contains(&season.anilist_id.to_ascii_lowercase());
        let mut mappings = mappings_by_anilist_id
            .iter()
            .filter(|(requested_id, mapping)| {
                let identity_matches =
                    requested_id
                        .trim()
                        .eq_ignore_ascii_case(season.anilist_id.trim())
                        || mapping.ids.anilist.as_deref().is_some_and(|id| {
                            id.trim().eq_ignore_ascii_case(season.anilist_id.trim())
                        });
                identity_matches
                    && (relation_identity
                        || anizip_mapping_contains_season(mapping, season.season_number)
                        || infer_anizip_mapping_season(mapping) == Some(season.season_number))
            })
            .map(|(requested_id, mapping)| {
                let fingerprint = serde_json::to_string(mapping.as_ref()).unwrap_or_default();
                (requested_id.clone(), fingerprint, mapping.as_ref().clone())
            })
            .collect::<Vec<_>>();
        mappings.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
        mappings.dedup_by(|left, right| left.1 == right.1);
        if mappings.is_empty() {
            inputs.push(LibraryAnimeMatchSeasonInput {
                season,
                mapping: None,
            });
        } else {
            for (_, _, mapping) in mappings {
                inputs.push(LibraryAnimeMatchSeasonInput {
                    season: season.clone(),
                    mapping: Some(mapping),
                });
            }
        }
    }
    inputs
}

fn library_anime_graph_fingerprint(seasons: &[LibraryAnimeMatchSeasonInput]) -> String {
    let mut evidence = Vec::new();
    for input in seasons {
        evidence.push(format!(
            "season:{}:{}:{}",
            input.season.season_number,
            input.season.anilist_id.trim(),
            input.season.title.trim()
        ));
        if let Some(mapping) = input.mapping.as_ref() {
            for target in build_mapping_targets("", input.season.season_number, mapping) {
                evidence.push(format!(
                    "target:{}:{:?}:{:?}:{:?}:{}:{}",
                    target.target_key,
                    target.season_number,
                    target.episode_number,
                    target.absolute_episode_number,
                    target.tvdb_episode_id.unwrap_or_default(),
                    target.anidb_episode_id.unwrap_or_default(),
                ));
            }
        }
    }
    evidence.sort_unstable();
    evidence.dedup();
    format!(
        "library-v1-{}",
        blake3::hash(evidence.join("\n").as_bytes()).to_hex()
    )
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredLibraryClassificationHint {
    library_type: ClassifierLibraryType,
    title: String,
    #[serde(default)]
    alt_titles: Vec<String>,
    season: Option<i32>,
    episode: Option<i32>,
    absolute_episode: Option<i32>,
}

fn stored_library_classification_hint(
    outcome: &ClassificationOutcome,
) -> Option<StoredLibraryClassificationHint> {
    outcome
        .hint_json
        .as_deref()
        .and_then(|value| serde_json::from_str::<StoredLibraryClassificationHint>(value).ok())
}

fn library_anime_parse_facts(
    file: &AggregatedFile,
    outcome: Option<&ClassificationOutcome>,
    canonical_title: &str,
) -> AnimeMatchParseFacts {
    let mut facts = AnimeMatchParseFacts::default();
    facts.title_candidates.push(canonical_title.to_string());
    if let Some(number) = file.season.filter(|number| *number >= 0) {
        facts.season_numbers.push(number);
    }
    if let Some(number) = file.episode.filter(|number| *number > 0) {
        facts.episode_numbers.push(number);
    }
    if let Some(number) = file.absolute_episode.filter(|number| *number > 0) {
        facts.absolute_episode_numbers.push(number);
    }
    if let Some(outcome) = outcome {
        if let Some(number) = outcome.season_scope.filter(|number| *number >= 0) {
            facts.season_numbers.push(number);
        }
        let stored_hint = stored_library_classification_hint(outcome);
        let title = outcome
            .parsed_hint
            .as_ref()
            .map(|hint| hint.title.clone())
            .or_else(|| stored_hint.as_ref().map(|hint| hint.title.clone()));
        if let Some(title) = title {
            facts.title_candidates.push(title);
        }
        facts.title_candidates.extend(
            outcome
                .parsed_hint
                .as_ref()
                .map(|hint| hint.alt_titles.clone())
                .or_else(|| stored_hint.as_ref().map(|hint| hint.alt_titles.clone()))
                .unwrap_or_default(),
        );
        let season = outcome
            .parsed_hint
            .as_ref()
            .and_then(|hint| hint.season)
            .or_else(|| stored_hint.as_ref().and_then(|hint| hint.season));
        let episode = outcome
            .parsed_hint
            .as_ref()
            .and_then(|hint| hint.episode)
            .or_else(|| stored_hint.as_ref().and_then(|hint| hint.episode));
        let absolute_episode = outcome
            .parsed_hint
            .as_ref()
            .and_then(|hint| hint.absolute_episode)
            .or_else(|| stored_hint.as_ref().and_then(|hint| hint.absolute_episode));
        if let Some(number) = season.filter(|number| *number >= 0) {
            facts.season_numbers.push(number);
        }
        if let Some(number) = episode.filter(|number| *number > 0) {
            facts.episode_numbers.push(number);
        }
        if let Some(number) = absolute_episode.filter(|number| *number > 0) {
            facts.absolute_episode_numbers.push(number);
        }
    }
    let filename = Path::new(&file.descriptor.path)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(&file.descriptor.path)
        .to_ascii_lowercase();
    if filename.contains("dual audio") || filename.contains("dual-audio") {
        facts.audio_profiles.push("dual_audio".to_string());
    }
    if filename.contains("dubbed") || filename.contains(" dub ") {
        facts.audio_profiles.push("dubbed".to_string());
    }
    if filename.contains("subbed") || filename.contains(" eng sub") {
        facts.audio_profiles.push("subbed".to_string());
    }
    facts.batch_kind = Some("single".to_string());
    facts
}

fn library_candidate_may_be_anime(
    candidate: &AggregatedCandidate,
    outcomes: &HashMap<String, ClassificationOutcome>,
) -> bool {
    if candidate.identity.r#type == MediaType::Anime {
        return true;
    }
    outcomes.values().any(|outcome| {
        outcome
            .parsed_hint
            .as_ref()
            .map(|hint| hint.library_type)
            .or_else(|| stored_library_classification_hint(outcome).map(|hint| hint.library_type))
            == Some(ClassifierLibraryType::Anime)
    })
}

fn external_ids_have_anime_identity(ids: &ExternalIds) -> bool {
    ids.anilist.is_some() || ids.anidb.is_some() || ids.mal.is_some() || ids.kitsu.is_some()
}

fn final_applied_classification_prefers_anime(
    outcomes: &HashMap<String, ClassificationOutcome>,
) -> bool {
    outcomes.values().any(|outcome| {
        outcome.disposition.is_applied()
            && (outcome
                .parsed_hint
                .as_ref()
                .map(|hint| hint.library_type)
                .or_else(|| {
                    stored_library_classification_hint(outcome).map(|hint| hint.library_type)
                })
                == Some(ClassifierLibraryType::Anime)
                || applied_classifier_identity_claim(outcome).is_some_and(|claim| {
                    claim.series_ids.iter().any(|(provider, _)| {
                        matches!(provider.as_str(), "anilist" | "anidb" | "mal" | "kitsu")
                    })
                }))
    })
}

fn retain_final_applied_classifier_state(
    base: &ExternalIds,
    classifier_emitted: &ExternalIds,
    current: &mut ExternalIds,
    season_anilist_seeds: &mut HashMap<i32, SeasonAnilistSeed>,
    outcomes: &HashMap<String, ClassificationOutcome>,
) {
    let applied_claims = outcomes
        .values()
        .filter_map(applied_classifier_identity_claim)
        .flat_map(|claim| claim.series_ids)
        .collect::<BTreeSet<_>>();
    let unresolved_claims = outcomes
        .values()
        .filter(|outcome| !outcome.disposition.is_applied())
        .filter_map(classifier_identity_claim)
        .flat_map(|claim| claim.series_ids)
        .collect::<BTreeSet<_>>();

    let claims_contain =
        |claims: &BTreeSet<(String, String)>, provider: &str, external_id: &str| {
            claims.iter().any(|(claimed_provider, claimed_id)| {
                claimed_provider == provider
                    && claimed_id.trim().eq_ignore_ascii_case(external_id.trim())
            })
        };
    let retain_field = |provider: &str,
                        base_value: &Option<String>,
                        emitted_value: &Option<String>,
                        current_value: &mut Option<String>| {
        let emitted_by_classifier = emitted_value.as_ref().is_some_and(|emitted| {
            base_value
                .as_ref()
                .is_none_or(|base| !base.trim().eq_ignore_ascii_case(emitted.trim()))
                && current_value
                    .as_ref()
                    .is_some_and(|current| current.trim().eq_ignore_ascii_case(emitted.trim()))
        });
        if emitted_by_classifier
            && !emitted_value
                .as_deref()
                .is_some_and(|external_id| claims_contain(&applied_claims, provider, external_id))
            && emitted_value.as_deref().is_some_and(|external_id| {
                claims_contain(&unresolved_claims, provider, external_id)
            })
        {
            *current_value = base_value.clone();
        }
    };

    retain_field(
        "imdb",
        &base.imdb,
        &classifier_emitted.imdb,
        &mut current.imdb,
    );
    retain_field(
        "tmdb",
        &base.tmdb,
        &classifier_emitted.tmdb,
        &mut current.tmdb,
    );
    retain_field(
        "tvdb",
        &base.tvdb,
        &classifier_emitted.tvdb,
        &mut current.tvdb,
    );
    retain_field(
        "tvdb",
        &base.tvdb_series,
        &classifier_emitted.tvdb_series,
        &mut current.tvdb_series,
    );
    retain_field(
        "tvdb",
        &base.tvdb_movie,
        &classifier_emitted.tvdb_movie,
        &mut current.tvdb_movie,
    );
    retain_field(
        "anilist",
        &base.anilist,
        &classifier_emitted.anilist,
        &mut current.anilist,
    );
    retain_field(
        "anidb",
        &base.anidb,
        &classifier_emitted.anidb,
        &mut current.anidb,
    );
    retain_field("mal", &base.mal, &classifier_emitted.mal, &mut current.mal);
    retain_field(
        "kitsu",
        &base.kitsu,
        &classifier_emitted.kitsu,
        &mut current.kitsu,
    );

    let causal_anilist_ids = season_anilist_seeds
        .values()
        .filter(|seed| !seed.causal_paths.is_empty())
        .map(|seed| seed.anilist_id.trim().to_string())
        .collect::<BTreeSet<_>>();
    let applied_paths = outcomes
        .iter()
        .filter_map(|(path, outcome)| outcome.disposition.is_applied().then_some(path.clone()))
        .collect::<BTreeSet<_>>();
    season_anilist_seeds.retain(|_, seed| {
        if seed.causal_paths.is_empty() {
            return true;
        }
        seed.causal_paths
            .retain(|path| applied_paths.contains(path));
        !seed.causal_paths.is_empty()
    });
    if current.anilist.as_deref().is_some_and(|current_anilist| {
        base.anilist.as_deref().is_none_or(|base_anilist| {
            !base_anilist
                .trim()
                .eq_ignore_ascii_case(current_anilist.trim())
        }) && causal_anilist_ids
            .iter()
            .any(|id| id.eq_ignore_ascii_case(current_anilist.trim()))
            && !season_anilist_seeds.values().any(|seed| {
                seed.anilist_id
                    .trim()
                    .eq_ignore_ascii_case(current_anilist.trim())
            })
    }) {
        current.anilist = base.anilist.clone();
    }
}

fn library_provisional_anilist_ids(
    outcomes: &HashMap<String, ClassificationOutcome>,
) -> BTreeSet<String> {
    fn collect(value: &serde_json::Value, ids: &mut BTreeSet<String>) {
        match value {
            serde_json::Value::Object(object) => {
                for (key, value) in object {
                    if matches!(key.as_str(), "anilist" | "anilistId" | "anilist_id") {
                        if let Some(id) = value.as_str().map(str::trim).filter(|id| !id.is_empty())
                        {
                            ids.insert(id.to_string());
                        }
                    } else {
                        collect(value, ids);
                    }
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    collect(value, ids);
                }
            }
            _ => {}
        }
    }

    let mut ids = BTreeSet::new();
    for outcome in outcomes.values() {
        if let Some(value) = outcome
            .candidates_json
            .as_deref()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        {
            collect(&value, &mut ids);
        }
    }
    ids.into_iter().take(8).collect()
}

fn library_anime_wanted_targets(
    canonical_title: &str,
    seasons: &[LibraryAnimeMatchSeasonInput],
    facts: &AnimeMatchParseFacts,
) -> Vec<LibraryAnimeTargetEvidence> {
    let season_numbers = facts
        .season_numbers
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let episode_numbers = facts
        .episode_numbers
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let absolute_numbers = facts
        .absolute_episode_numbers
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let has_number_evidence =
        !season_numbers.is_empty() || !episode_numbers.is_empty() || !absolute_numbers.is_empty();
    let mut ranked = BTreeMap::<String, (u8, LibraryAnimeTargetEvidence)>::new();
    for input in seasons {
        let Some(mapping) = input.mapping.as_ref() else {
            continue;
        };
        for target in build_mapping_targets(canonical_title, input.season.season_number, mapping) {
            let season_matches = target
                .season_number
                .is_some_and(|number| season_numbers.contains(&number));
            let episode_matches = target
                .episode_number
                .is_some_and(|number| episode_numbers.contains(&number));
            let absolute_matches = target
                .absolute_episode_number
                .is_some_and(|number| absolute_numbers.contains(&number));
            let cross_number_matches = target
                .absolute_episode_number
                .is_some_and(|number| episode_numbers.contains(&number))
                || target
                    .episode_number
                    .is_some_and(|number| absolute_numbers.contains(&number));
            let score = if season_matches && episode_matches {
                100
            } else if absolute_matches {
                95
            } else if episode_matches && season_numbers.is_empty() {
                85
            } else if episode_matches {
                75
            } else if cross_number_matches {
                65
            } else if season_matches && episode_numbers.is_empty() && absolute_numbers.is_empty() {
                45
            } else if !has_number_evidence {
                10
            } else {
                0
            };
            if score == 0 {
                continue;
            }
            let evidence = LibraryAnimeTargetEvidence {
                target_key: target.target_key.clone(),
                season_number: target.season_number,
                episode_number: target.episode_number,
                absolute_episode_number: target.absolute_episode_number,
            };
            let replace = ranked
                .get(&target.target_key)
                .is_none_or(|(current_score, current)| {
                    score > *current_score
                        || (score == *current_score
                            && (
                                evidence.season_number,
                                evidence.episode_number,
                                evidence.absolute_episode_number,
                            ) < (
                                current.season_number,
                                current.episode_number,
                                current.absolute_episode_number,
                            ))
                });
            if replace {
                ranked.insert(target.target_key, (score, evidence));
            }
        }
    }
    let mut ranked = ranked.into_values().collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| left.1.season_number.cmp(&right.1.season_number))
            .then_with(|| left.1.episode_number.cmp(&right.1.episode_number))
            .then_with(|| {
                left.1
                    .absolute_episode_number
                    .cmp(&right.1.absolute_episode_number)
            })
            .then_with(|| left.1.target_key.cmp(&right.1.target_key))
    });
    ranked
        .into_iter()
        .take(LIBRARY_ANIME_MATCH_MAX_WANTED_TARGETS)
        .map(|(_, target)| target)
        .collect()
}

fn merge_library_anime_match_provenance(
    existing: Option<&str>,
    provenance: &AnimeMatchAssistProvenance,
) -> String {
    let mut value = existing
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .filter(serde_json::Value::is_object)
        .unwrap_or_else(|| serde_json::json!({}));
    if let (Some(object), Some(assist)) = (
        value.as_object_mut(),
        provenance.as_json().get("animeMatchAssist").cloned(),
    ) {
        object.insert("animeMatchAssist".to_string(), assist);
    }
    serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_string())
}

fn episode_number_evidence(
    file: &AggregatedFile,
    outcome: Option<&ClassificationOutcome>,
) -> ResolvedEpisodeNumbers {
    let Some(outcome) = outcome.filter(|outcome| outcome.disposition.is_applied()) else {
        return ResolvedEpisodeNumbers {
            season: None,
            episode: None,
            absolute_episode: None,
        };
    };
    if outcome.preserve_authoritative_episode_links {
        return ResolvedEpisodeNumbers {
            season: None,
            episode: None,
            absolute_episode: None,
        };
    }

    let accepted = outcome.accepted_numbers;
    let mut season = accepted.and_then(|numbers| numbers.season).or(file.season);
    let mut episode = accepted
        .and_then(|numbers| numbers.episode)
        .or(file.episode);
    let mut absolute_episode = accepted
        .and_then(|numbers| numbers.absolute_episode)
        .or(file.absolute_episode);

    if let Some(hint) = outcome.parsed_hint.as_ref() {
        if season.is_none() {
            season = hint.season;
        }
        if episode.is_none() {
            episode = hint.episode;
        }
        if absolute_episode.is_none() {
            absolute_episode = hint.absolute_episode;
        }
    }

    ResolvedEpisodeNumbers {
        season,
        episode,
        absolute_episode,
    }
}

fn insert_anizip_episode_numbers(
    number_map: &mut CanonicalEpisodeNumberMap,
    mapping: &AniZipMapping,
) {
    for episode in &mapping.episodes {
        let (Some(season), Some(episode), Some(absolute_episode)) = (
            episode.season_number,
            episode.episode_number,
            episode.absolute_episode_number,
        ) else {
            continue;
        };
        if season < 0 || episode <= 0 || absolute_episode <= 0 {
            continue;
        }
        let canonical = CanonicalEpisodeNumber {
            season,
            episode,
            absolute_episode,
        };
        let season_numbers = number_map.entry(season).or_default();
        if !season_numbers.contains(&canonical) {
            season_numbers.push(canonical);
        }
    }
}

fn merge_authoritative_anizip_numbers(
    mut persisted: CanonicalEpisodeNumberMap,
    current: CanonicalEpisodeNumberMap,
) -> CanonicalEpisodeNumberMap {
    let current_slots: BTreeSet<(i32, i32)> = current
        .values()
        .flatten()
        .map(|entry| (entry.season, entry.episode))
        .collect();
    let current_absolute_numbers: BTreeSet<i32> = current
        .values()
        .flatten()
        .map(|entry| entry.absolute_episode)
        .collect();

    // Current evidence supersedes an older canonical slot or absolute number. Unrelated
    // persisted entries remain available when a provider response is partial.
    for entries in persisted.values_mut() {
        entries.retain(|entry| {
            !current_slots.contains(&(entry.season, entry.episode))
                && !current_absolute_numbers.contains(&entry.absolute_episode)
        });
    }
    persisted.retain(|_, entries| !entries.is_empty());

    // Preserve conflicts between current mappings. The canonical lookup deliberately
    // rejects multiple matches instead of allowing iteration order to pick a winner.
    for (season, entries) in current {
        let stored = persisted.entry(season).or_default();
        for entry in entries {
            if !stored.contains(&entry) {
                stored.push(entry);
            }
        }
    }
    persisted
}

fn merge_root_anizip_external_ids(
    ids: &ExternalIds,
    mappings: &HashMap<String, Arc<AniZipMapping>>,
) -> ExternalIds {
    let Some(root_anilist_id) = ids
        .anilist
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return ids.clone();
    };
    let Some(root_mapping) = mappings.get(root_anilist_id) else {
        return ids.clone();
    };
    merge_external_ids(ids, Some(root_mapping.ids.clone()))
}

fn lookup_canonical_absolute_episode(
    number_map: &CanonicalEpisodeNumberMap,
    expected_season: Option<i32>,
    expected_episode: Option<i32>,
    absolute_episode: i32,
) -> Option<(i32, i32)> {
    let mut matches = BTreeSet::new();
    let seasons: Box<dyn Iterator<Item = &Vec<CanonicalEpisodeNumber>> + '_> =
        if let Some(season) = expected_season {
            Box::new(number_map.get(&season).into_iter())
        } else {
            Box::new(number_map.values())
        };
    for entries in seasons {
        for entry in entries {
            if entry.absolute_episode != absolute_episode
                || expected_season.is_some_and(|season| season != entry.season)
                || expected_episode.is_some_and(|episode| episode != entry.episode)
            {
                continue;
            }
            matches.insert((entry.season, entry.episode));
        }
    }
    if matches.len() == 1 {
        matches.into_iter().next()
    } else {
        None
    }
}

fn classification_disposition(
    match_opt: Option<&ClassifierCanonicalMatch>,
    winner_margin: Option<f32>,
) -> ClassificationDisposition {
    match match_opt {
        Some(matched)
            if matched.confidence >= CLASSIFICATION_APPLICATION_CONFIDENCE
                && winner_margin
                    .map(|margin| margin >= CLASSIFICATION_APPLICATION_MIN_MARGIN)
                    .unwrap_or(true) =>
        {
            ClassificationDisposition::Applied
        }
        _ => ClassificationDisposition::Unresolved,
    }
}

fn tvdb_anime_bridge_disposition(
    match_opt: Option<&ClassifierCanonicalMatch>,
    winner_margin: Option<f32>,
    seed_season: i32,
) -> ClassificationDisposition {
    if match_opt.and_then(|matched| matched.season) != Some(seed_season) {
        return ClassificationDisposition::Unresolved;
    }
    classification_disposition(match_opt, winner_margin)
}

fn build_classification_evidence_payloads(
    hint: &elixir_classifier::hint::ClassificationHint,
    hypotheses: &[serde_json::Value],
    runner_up_confidence: Option<f32>,
    winner_margin: Option<f32>,
) -> Result<(Option<String>, Option<String>)> {
    let hint_json = Some(serde_json::to_string(hint)?);
    let candidates_json = Some(
        serde_json::json!({
            "hypotheses": hypotheses,
            "runnerUpConfidence": runner_up_confidence,
            "winnerMargin": winner_margin,
        })
        .to_string(),
    );

    Ok((hint_json, candidates_json))
}

fn insert_season_anilist_seed(
    seeds: &mut HashMap<i32, SeasonAnilistSeed>,
    season_number: i32,
    mut seed: SeasonAnilistSeed,
) {
    seed.anilist_id = seed.anilist_id.trim().to_string();
    if seed.anilist_id.is_empty() || !seed.confidence.is_finite() {
        tracing::warn!(
            season = season_number,
            anilist_id = %seed.anilist_id,
            confidence = seed.confidence,
            "ignoring unusable anilist season seed"
        );
        return;
    }

    let Some(current) = seeds.get_mut(&season_number) else {
        tracing::trace!(
            season = season_number,
            anilist_id = %seed.anilist_id,
            confidence = seed.confidence,
            "season anilist seed stored"
        );
        seeds.insert(season_number, seed);
        return;
    };

    match seed.confidence.total_cmp(&current.confidence) {
        std::cmp::Ordering::Greater => {
            tracing::trace!(
                season = season_number,
                anilist_id = %seed.anilist_id,
                confidence = seed.confidence,
                "season anilist seed replaced by stronger evidence"
            );
            *current = seed;
        }
        std::cmp::Ordering::Equal if current.anilist_id != seed.anilist_id => {
            tracing::warn!(
                season = season_number,
                first_anilist_id = %current.anilist_id,
                conflicting_anilist_id = %seed.anilist_id,
                confidence = seed.confidence,
                "equal-confidence anilist season seeds conflict; suppressing automatic mapping"
            );
            current.anilist_id.clear();
            current.causal_paths.clear();
        }
        std::cmp::Ordering::Equal => {
            // Equal evidence for the same identity is shared provenance. Keep
            // every claimant so later causal-row capture can refuse to assign
            // the row to either sibling.
            current.causal_paths.extend(seed.causal_paths);
        }
        _ => {
            tracing::trace!(
                season = season_number,
                anilist_id = %current.anilist_id,
                confidence = current.confidence,
                "season anilist seed retained"
            );
        }
    }
}

fn apply_anilist_relation_chain_seeds(
    seeds: &mut HashMap<i32, SeasonAnilistSeed>,
    expanded: &[AniListSeasonChainEntry],
    causal_paths: &BTreeSet<String>,
) {
    let relation_ids = expanded
        .iter()
        .map(|entry| entry.anilist_id.trim().to_ascii_lowercase())
        .filter(|id| !id.is_empty())
        .collect::<HashSet<_>>();
    if relation_ids.is_empty() {
        return;
    }

    // Direct ani.zip/TVDB evidence may key an AniList work under the
    // provider's season number. Once a complete relation chain resolves that
    // work's canonical ordinal, discard only those stale placements before
    // inserting the relation identities. Unrelated direct evidence remains.
    let mut direct_evidence = HashMap::<String, SeasonAnilistSeed>::new();
    seeds.retain(|_, seed| {
        let id = seed.anilist_id.trim().to_ascii_lowercase();
        if !relation_ids.contains(&id) {
            return true;
        }
        if season_anilist_seed_is_usable(seed) {
            let evidence = direct_evidence.entry(id).or_insert_with(|| seed.clone());
            evidence.confidence = evidence.confidence.max(seed.confidence);
            evidence.causal_paths.extend(seed.causal_paths.clone());
        }
        false
    });
    for entry in expanded {
        let prior = direct_evidence.remove(&entry.anilist_id.trim().to_ascii_lowercase());
        let mut entry_causal_paths = causal_paths.clone();
        if let Some(prior) = prior.as_ref() {
            entry_causal_paths.extend(prior.causal_paths.clone());
        }
        insert_season_anilist_seed(
            seeds,
            entry.season_number,
            SeasonAnilistSeed {
                anilist_id: entry.anilist_id.clone(),
                confidence: prior
                    .as_ref()
                    .map(|prior| prior.confidence.max(entry.confidence))
                    .unwrap_or(entry.confidence),
                causal_paths: entry_causal_paths,
            },
        );
    }
}

fn season_anilist_seed_is_usable(seed: &SeasonAnilistSeed) -> bool {
    !seed.anilist_id.trim().is_empty() && seed.confidence.is_finite()
}

fn suppress_conflicting_classifier_anilist_id(
    base_ids: &ExternalIds,
    merged_ids: &mut ExternalIds,
    seeds: &HashMap<i32, SeasonAnilistSeed>,
    identity_is_authoritative: bool,
) {
    if identity_is_authoritative
        || base_ids
            .anilist
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
        || seeds.values().all(season_anilist_seed_is_usable)
    {
        return;
    }

    // Per-file classifier results are aggregated base-first. If equally strong files
    // disagree about a season, do not let whichever file happened to run first leak
    // through as the series/root AniList identity. A later relation expansion may still
    // establish a root from the remaining unambiguous season seeds.
    merged_ids.anilist = None;
}

async fn load_cached_anizip_mapping(
    pool: &AnyPool,
    anilist_id: &str,
    ttl_seconds: u64,
    force_refresh: bool,
) -> Result<Option<CachedAniZipMapping>> {
    let row = sqlx::query(
        "SELECT schema_version, mapping_json, fetched_at_epoch_seconds \
         FROM anizip_mapping_cache WHERE anilist_id = $1 LIMIT 1",
    )
    .bind(anilist_id)
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };

    let schema_version: i64 = row.try_get("schema_version")?;
    if schema_version != i64::from(ANIZIP_MAPPING_CACHE_SCHEMA_VERSION) {
        tracing::warn!(
            anilist_id,
            schema_version,
            expected_schema_version = ANIZIP_MAPPING_CACHE_SCHEMA_VERSION,
            "ignoring unsupported ani.zip mapping cache schema"
        );
        return Ok(None);
    }

    let mapping_json: String = row.try_get("mapping_json")?;
    let mapping = match serde_json::from_str::<AniZipMapping>(&mapping_json) {
        Ok(mapping) if anizip_mapping_has_content(&mapping) => mapping,
        Ok(_) => {
            tracing::warn!(anilist_id, "ignoring empty ani.zip mapping cache entry");
            return Ok(None);
        }
        Err(error) => {
            tracing::warn!(
                anilist_id,
                error = %error,
                "ignoring malformed ani.zip mapping cache entry"
            );
            return Ok(None);
        }
    };
    let fetched_at_epoch_seconds: i64 = row.try_get("fetched_at_epoch_seconds")?;
    let age_seconds = Utc::now()
        .timestamp()
        .saturating_sub(fetched_at_epoch_seconds);
    let is_fresh = !force_refresh
        && ttl_seconds > 0
        && age_seconds >= 0
        && age_seconds <= i64::try_from(ttl_seconds).unwrap_or(i64::MAX)
        && anizip_mapping_has_canonical_episode_numbers(&mapping);

    Ok(Some(CachedAniZipMapping { mapping, is_fresh }))
}

async fn persist_cached_anizip_mapping(
    pool: &AnyPool,
    anilist_id: &str,
    mapping: &AniZipMapping,
) -> Result<()> {
    if !anizip_mapping_has_content(mapping) {
        anyhow::bail!("refusing to persist an empty ani.zip mapping for {anilist_id}");
    }
    let mapping_json = serde_json::to_string(mapping)?;
    let previous = sqlx::query(
        "SELECT schema_version, mapping_json FROM anizip_mapping_cache \
         WHERE anilist_id = $1 LIMIT 1",
    )
    .bind(anilist_id)
    .fetch_optional(pool)
    .await?;
    let provider_data_changed = previous.is_some_and(|row| {
        row.try_get::<i64, _>("schema_version").ok()
            != Some(i64::from(ANIZIP_MAPPING_CACHE_SCHEMA_VERSION))
            || row.try_get::<String, _>("mapping_json").ok().as_deref()
                != Some(mapping_json.as_str())
    });
    let now_epoch_seconds = Utc::now().timestamp();
    sqlx::query::<sqlx::Any>(
        "INSERT INTO anizip_mapping_cache \
         (anilist_id, schema_version, mapping_json, fetched_at_epoch_seconds, \
          updated_at_epoch_seconds) \
         VALUES ($1, $2, $3, $4, $4) \
         ON CONFLICT(anilist_id) DO UPDATE SET \
         schema_version = excluded.schema_version, mapping_json = excluded.mapping_json, \
         fetched_at_epoch_seconds = excluded.fetched_at_epoch_seconds, \
         updated_at_epoch_seconds = excluded.updated_at_epoch_seconds",
    )
    .bind(anilist_id)
    .bind(ANIZIP_MAPPING_CACHE_SCHEMA_VERSION)
    .bind(mapping_json)
    .bind(now_epoch_seconds)
    .execute(pool)
    .await?;
    if provider_data_changed {
        request_anime_library_repair_after_provider_correction();
    }
    Ok(())
}

async fn anizip_mapping_for_scan(
    pool: &AnyPool,
    linker: Option<&LinkerService>,
    anilist_id: &str,
    ttl_seconds: u64,
    force_refresh: bool,
    scan_cache: &mut HashMap<String, Option<AniZipMapping>>,
) -> Result<Option<AniZipMapping>> {
    let normalized_id = anilist_id.trim();
    if normalized_id.is_empty() {
        return Ok(None);
    }
    if let Some(cached) = scan_cache.get(normalized_id) {
        return Ok(cached.clone());
    }

    let persisted =
        load_cached_anizip_mapping(pool, normalized_id, ttl_seconds, force_refresh).await?;
    let persisted_is_fresh = persisted.as_ref().is_some_and(|entry| entry.is_fresh);
    let mut resolved = persisted.map(|entry| entry.mapping);

    if !persisted_is_fresh {
        if let Some(linker) = linker {
            match linker.fetch_anizip_mapping(normalized_id).await {
                Ok(Some(mapping)) if anizip_mapping_has_canonical_episode_numbers(&mapping) => {
                    persist_cached_anizip_mapping(pool, normalized_id, &mapping).await?;
                    resolved = Some(mapping);
                }
                Ok(Some(mapping)) if anizip_mapping_has_content(&mapping) => {
                    let retained_canonical = resolved
                        .as_ref()
                        .is_some_and(anizip_mapping_has_canonical_episode_numbers);
                    if !retained_canonical {
                        resolved = Some(mapping);
                    }
                    tracing::warn!(
                        anilist_id = normalized_id,
                        retained_canonical,
                        "ani.zip refresh lacked canonical episode numbers; retaining cached canonical mapping when available"
                    );
                }
                Ok(Some(_)) => {
                    tracing::warn!(
                        anilist_id = normalized_id,
                        has_stale_cache = resolved.is_some(),
                        "ani.zip returned an empty mapping; retaining cached mapping when available"
                    );
                }
                Ok(None) => {
                    tracing::warn!(
                        anilist_id = normalized_id,
                        has_stale_cache = resolved.is_some(),
                        "ani.zip mapping was not found; retaining cached mapping when available"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        anilist_id = normalized_id,
                        has_stale_cache = resolved.is_some(),
                        error = %error,
                        "ani.zip mapping refresh failed; retaining cached mapping when available"
                    );
                }
            }
        }
    }

    scan_cache.insert(normalized_id.to_string(), resolved.clone());
    Ok(resolved)
}

fn anizip_mapping_has_content(mapping: &AniZipMapping) -> bool {
    mapping.episodes.iter().any(|episode| {
        episode.season_number.is_some_and(|value| value >= 0)
            && episode.episode_number.is_some_and(|value| value > 0)
            || episode
                .absolute_episode_number
                .is_some_and(|value| value > 0)
    }) || mapping.images.iter().any(|image| {
        image
            .url
            .as_deref()
            .is_some_and(|url| !url.trim().is_empty())
    }) || mapping
        .titles
        .values()
        .any(|title| !title.trim().is_empty())
        || [
            mapping.ids.imdb.as_deref(),
            mapping.ids.tmdb.as_deref(),
            mapping.ids.tvdb.as_deref(),
            mapping.ids.tvdb_series.as_deref(),
            mapping.ids.tvdb_movie.as_deref(),
            mapping.ids.anilist.as_deref(),
            mapping.ids.anidb.as_deref(),
            mapping.ids.mal.as_deref(),
            mapping.ids.kitsu.as_deref(),
        ]
        .into_iter()
        .flatten()
        .any(|value| !value.trim().is_empty())
}

fn anizip_mapping_has_canonical_episode_numbers(mapping: &AniZipMapping) -> bool {
    mapping.episodes.iter().any(|episode| {
        episode.season_number.is_some_and(|value| value >= 0)
            && episode.episode_number.is_some_and(|value| value > 0)
            && episode
                .absolute_episode_number
                .is_some_and(|value| value > 0)
    })
}

async fn load_persisted_episode_number_map(
    pool: &AnyPool,
    series_id: Uuid,
) -> Result<CanonicalEpisodeNumberMap> {
    let rows = sqlx::query(
        "SELECT s.season_number, aem.episode_number, aem.raw_json \
         FROM anime_episode_meta aem \
         INNER JOIN seasons s ON s.id = aem.season_id \
         WHERE s.series_id = $1 AND aem.raw_json IS NOT NULL \
         ORDER BY s.season_number, aem.episode_number",
    )
    .bind(series_id.to_string())
    .fetch_all(pool)
    .await?;
    let mut number_map = CanonicalEpisodeNumberMap::new();
    for row in rows {
        let season = row.try_get::<i64, _>("season_number")? as i32;
        let episode = row.try_get::<i64, _>("episode_number")? as i32;
        let raw_json: String = row.try_get("raw_json")?;
        let Some(absolute_episode) = persisted_anizip_absolute_episode(&raw_json) else {
            continue;
        };
        if season < 0 || episode <= 0 || absolute_episode <= 0 {
            continue;
        }
        number_map
            .entry(season)
            .or_default()
            .push(CanonicalEpisodeNumber {
                season,
                episode,
                absolute_episode,
            });
    }
    Ok(number_map)
}

fn persisted_anizip_absolute_episode(raw_json: &str) -> Option<i32> {
    let raw = serde_json::from_str::<serde_json::Value>(raw_json).ok()?;
    for key in ["absoluteEpisodeNumber", "absolute_episode_number"] {
        if let Some(number) = raw.get(key).and_then(json_positive_i32) {
            return Some(number);
        }
    }
    raw.get("episode").and_then(json_positive_i32)
}

fn json_positive_i32(value: &serde_json::Value) -> Option<i32> {
    value
        .as_i64()
        .and_then(|number| i32::try_from(number).ok())
        .or_else(|| value.as_u64().and_then(|number| i32::try_from(number).ok()))
        .or_else(|| value.as_str()?.trim().parse::<i32>().ok())
        .filter(|number| *number > 0)
}

fn infer_anizip_mapping_season(mapping: &AniZipMapping) -> Option<i32> {
    let seasons: BTreeSet<i32> = mapping
        .episodes
        .iter()
        .filter(|episode| episode.episode_number.is_some_and(|number| number > 0))
        .filter_map(|episode| episode.season_number)
        .filter(|season| *season > 0)
        .collect();
    if seasons.len() == 1 {
        seasons.into_iter().next()
    } else {
        None
    }
}

fn anizip_mapping_contains_season(mapping: &AniZipMapping, season_number: i32) -> bool {
    mapping.episodes.iter().any(|episode| {
        episode.season_number == Some(season_number)
            && episode.episode_number.is_some_and(|number| number > 0)
    })
}

fn anizip_mapping_contains_relation_season(mapping: &AniZipMapping, relation_season: i32) -> bool {
    let prefer_mainline_numbering = anizip_prefers_mainline_numbering(mapping);
    mapping.episodes.iter().any(|episode| {
        resolve_anizip_target_numbers(relation_season, prefer_mainline_numbering, episode).0
            == Some(relation_season)
    })
}

pub(crate) fn anizip_prefers_mainline_numbering(mapping: &AniZipMapping) -> bool {
    let mut structured_count = 0_usize;
    let mut absolute_only_count = 0_usize;
    for episode in &mapping.episodes {
        if episode.season_number.is_some() || episode.episode_number.is_some() {
            structured_count += 1;
        } else if episode.mainline_episode_number.is_some() {
            absolute_only_count += 1;
        }
    }
    absolute_only_count > structured_count
}

pub(crate) fn resolve_anizip_target_numbers(
    relation_season: i32,
    prefer_mainline_numbering: bool,
    episode: &AniZipEpisodeRecord,
) -> (Option<i32>, Option<i32>, Option<i32>) {
    let mainline_episode = episode.mainline_episode_number.filter(|number| *number > 0);
    let structured_season = episode.season_number.filter(|number| *number >= 0);
    let provider_season = structured_season.filter(|number| *number > 0);
    let relation_season = (relation_season > 0).then_some(relation_season);
    let absolute_episode = episode.absolute_episode_number.filter(|number| *number > 0);

    // A single AniList work can be serialized as a later block inside one
    // TVDB season. A numeric ani.zip episode label is the work-local ordinal;
    // when the provider season disagrees with the resolved relation work, use
    // that local ordinal under the relation season while retaining absolute
    // and provider episode IDs as independent evidence.
    if !prefer_mainline_numbering
        && let (Some(relation_season), Some(provider_season), Some(local_episode)) =
            (relation_season, provider_season, mainline_episode)
        && relation_season != provider_season
    {
        return (Some(relation_season), Some(local_episode), absolute_episode);
    }

    let use_mainline_numbering = prefer_mainline_numbering && mainline_episode.is_some();
    let has_structured_numbering =
        episode.season_number.is_some() || episode.episode_number.is_some();
    let season_number = if use_mainline_numbering {
        None
    } else if has_structured_numbering {
        structured_season.or(relation_season)
    } else {
        None
    };
    let episode_number = if use_mainline_numbering {
        None
    } else {
        episode.episode_number.filter(|number| *number > 0)
    };
    let absolute_episode_number = if use_mainline_numbering {
        mainline_episode
    } else {
        absolute_episode
    };
    (season_number, episode_number, absolute_episode_number)
}

fn select_root_anilist_id(
    expanded: &[AniListSeasonChainEntry],
    seeds: &HashMap<i32, SeasonAnilistSeed>,
) -> Option<String> {
    if let Some(entry) = expanded.iter().min_by_key(|entry| entry.season_number) {
        return Some(entry.anilist_id.clone());
    }
    seeds
        .iter()
        .filter(|(_, seed)| season_anilist_seed_is_usable(seed))
        .min_by_key(|(season, _)| *season)
        .map(|(_, seed)| seed.anilist_id.clone())
}

fn is_anilist_season_format(format: Option<&str>) -> bool {
    matches!(format, Some("TV") | Some("TV_SHORT") | Some("ONA"))
}

async fn expand_anilist_season_chain(
    anilist: &AniListIdentifier,
    seed_season: i32,
    seed: &SeasonAnilistSeed,
) -> Result<Vec<AniListSeasonChainEntry>> {
    let Ok(seed_id) = seed.anilist_id.parse::<i32>() else {
        tracing::warn!(
            anilist_id = %seed.anilist_id,
            "invalid anilist id for season chain"
        );
        return Ok(Vec::new());
    };

    let chain = anilist.relation_chain(seed_id).await?;
    if chain.is_empty() {
        return Ok(Vec::new());
    }

    for node in &chain {
        tracing::trace!(
            seed_anilist_id = %seed.anilist_id,
            node_id = node.id,
            node_format = ?node.format,
            node_title = %node.title,
            "anilist chain node"
        );
    }

    let expanded = expand_anilist_season_chain_nodes(&chain, seed_season, seed);
    if !expanded.is_empty() {
        tracing::trace!(
            seed_anilist_id = %seed.anilist_id,
            seasons = expanded.len(),
            "anilist season chain resolved"
        );
    }

    Ok(expanded)
}

fn expand_anilist_season_chain_nodes(
    chain: &[AniListRelationNode],
    seed_season: i32,
    seed: &SeasonAnilistSeed,
) -> Vec<AniListSeasonChainEntry> {
    let Ok(seed_id) = seed.anilist_id.parse::<i32>() else {
        return Vec::new();
    };

    let mut filtered: Vec<_> = chain
        .iter()
        .cloned()
        .filter(|node| is_anilist_season_format(node.format.as_deref()))
        .collect();

    if filtered.iter().all(|node| node.id != seed_id) {
        let fallback: Vec<_> = chain
            .iter()
            .cloned()
            .filter(|node| {
                is_anilist_season_format(node.format.as_deref()) || node.format.is_none()
            })
            .collect();
        if fallback.iter().any(|node| node.id == seed_id) {
            tracing::warn!(
                anilist_id = %seed.anilist_id,
                "anilist chain formats missing; using fallback filter"
            );
            filtered = fallback;
        }
    }

    if filtered.is_empty() {
        return Vec::new();
    }

    let seed_index = match filtered.iter().position(|node| node.id == seed_id) {
        Some(idx) => idx,
        None => {
            tracing::warn!(
                anilist_id = %seed.anilist_id,
                "anilist seed id not found in relation chain"
            );
            return Vec::new();
        }
    };

    let relation_seed_ordinal = seed_index as i32 + 1;
    if relation_seed_ordinal < seed_season.max(1) {
        tracing::warn!(
            anilist_id = %seed.anilist_id,
            seed_season,
            relation_seed_ordinal,
            "anilist relation chain is missing known seasonal predecessors; retaining bounded seed context"
        );
        return Vec::new();
    }

    let mut expanded = Vec::new();
    for (idx, node) in filtered.iter().enumerate() {
        // Relation-season identity and provider episode numbering are separate
        // axes. Once the predecessor chain reaches the known seed ordinal, the
        // ordered AniList seasonal works define identity S1..Sn. ani.zip/TVDB
        // episode season and absolute numbers remain untouched downstream.
        let season_number = idx as i32 + 1;
        let confidence = if node.id == seed_id {
            seed.confidence
        } else {
            seed.confidence * 0.8
        };
        expanded.push(AniListSeasonChainEntry {
            season_number,
            anilist_id: node.id.to_string(),
            title: node.title.clone(),
            format: node.format.clone(),
            season_year: node.season_year,
            start_year: node.start_year,
            status: node.status.clone(),
            episodes: node.episodes,
            next_airing_episode: node
                .next_airing_episode
                .as_ref()
                .map(|episode| episode.episode),
            next_airing_at: node
                .next_airing_episode
                .as_ref()
                .map(|episode| episode.airing_at),
            confidence,
        });
    }
    expanded
}

pub async fn resolve_anilist_season_chain(
    config: Option<&ClassifierConfig>,
    seed_season: i32,
    anilist_id: &str,
    confidence: f32,
) -> Result<Vec<AniListSeasonChainEntry>> {
    let anilist = build_anilist_identifier(config);
    let seed = SeasonAnilistSeed {
        anilist_id: anilist_id.trim().to_string(),
        confidence,
        causal_paths: BTreeSet::new(),
    };
    expand_anilist_season_chain(&anilist, seed_season.max(1), &seed).await
}

fn build_anilist_identifier(config: Option<&ClassifierConfig>) -> AniListIdentifier {
    let timeout = config.map(|cfg| cfg.request_timeout_seconds).unwrap_or(10);
    AniListIdentifier::new(ANILIST_ENDPOINT.to_string(), timeout)
}

fn mark_tvdb_anime_bridge_prerequisite_unresolved(
    classification_outcomes: &mut HashMap<String, ClassificationOutcome>,
    tvdb_seeds: &HashMap<i32, TvdbBridgeSeed>,
    reason: &str,
) -> Result<()> {
    let mut seeds: Vec<_> = tvdb_seeds.values().collect();
    seeds.sort_by_key(|seed| seed.season_number);
    for seed in seeds {
        let hint_json = Some(serde_json::to_string(&seed.hint)?);
        let candidates_json = Some(
            serde_json::json!({
                "stage": "tvdb_to_anilist",
                "hypotheses": [],
                "runnerUpConfidence": null,
                "winnerMargin": null,
                "classificationError": reason,
            })
            .to_string(),
        );
        apply_tvdb_anime_bridge_outcome(
            classification_outcomes,
            seed.season_number,
            ClassificationDisposition::Unresolved,
            None,
            hint_json,
            candidates_json,
        );
    }
    Ok(())
}

async fn apply_tvdb_anime_bridge(
    series_meta: &serde_json::Value,
    anilist: &AniListIdentifier,
    scorer: &DefaultScorer,
    merged_ids: &mut ExternalIds,
    classification_outcomes: &mut HashMap<String, ClassificationOutcome>,
    seed: &TvdbBridgeSeed,
    season_year: Option<i32>,
) -> Result<AnimeBridgeResult> {
    let mut result = AnimeBridgeResult::default();
    result.prefer_anime = true;

    let tvdb_id = merged_ids
        .tvdb_series
        .as_ref()
        .or(merged_ids.tvdb.as_ref())
        .cloned()
        .unwrap_or_default();
    tracing::debug!(tvdb_id = %tvdb_id, season = seed.season_number, "tvdb indicates anime; attempting anilist bridge");

    let seed_hint = &seed.hint;
    let title = extract_tvdb_title(series_meta)
        .or_else(|| Some(seed_hint.title.clone()))
        .filter(|value| !value.trim().is_empty());
    let Some(title) = title else {
        return Ok(result);
    };

    let mut alt_titles = seed_hint.alt_titles.clone();
    if seed_hint.title != title {
        alt_titles.push(seed_hint.title.clone());
    }
    if seed.season_number > 1 {
        alt_titles.push(format!("{} Season {}", seed_hint.title, seed.season_number));
        if seed_hint.title != title {
            alt_titles.push(format!("{} Season {}", title, seed.season_number));
        }
    }
    alt_titles = dedupe_titles(alt_titles);

    let year = if seed.season_number > 1 {
        season_year
    } else {
        extract_tvdb_year(series_meta).or(seed_hint.year)
    };

    let mut hint = ClassifierHint {
        library_type: ClassifierLibraryType::Anime,
        title,
        alt_titles,
        year,
        season: Some(seed.season_number),
        episode: seed_hint.episode,
        absolute_episode: seed_hint.absolute_episode,
        duration_seconds: seed_hint.duration_seconds,
        embedded_ids: classifier_ids_from_server(merged_ids, MediaType::Anime),
        parser: "tvdb_bridge",
        parser_confidence: seed_hint.parser_confidence,
        source_path: seed_hint.source_path.clone(),
    };

    let candidates = match anilist.identify(&hint).await {
        Ok(items) => items,
        Err(err) => {
            tracing::warn!("tvdb anime bridge anilist lookup failed: {err}");
            let hint_json = Some(serde_json::to_string(&hint)?);
            let candidates_json = Some(
                serde_json::json!({
                    "stage": "tvdb_to_anilist",
                    "hypotheses": [],
                    "runnerUpConfidence": null,
                    "winnerMargin": null,
                    "classificationError": err.to_string(),
                })
                .to_string(),
            );
            apply_tvdb_anime_bridge_outcome(
                classification_outcomes,
                seed.season_number,
                ClassificationDisposition::Unresolved,
                None,
                hint_json,
                candidates_json,
            );
            return Ok(result);
        }
    };
    let base_canonical = scorer.score(&hint, &candidates);
    let mut bridge_results = vec![ClassifiedHint {
        hint: hint.clone(),
        canonical: base_canonical,
    }];

    if bridge_results
        .first()
        .and_then(|item| item.canonical.as_ref())
        .as_ref()
        .map(|value| value.confidence < 0.65)
        .unwrap_or(true)
    {
        tracing::trace!(
            tvdb_id = %tvdb_id,
            season = seed.season_number,
            confidence = bridge_results
                .first()
                .and_then(|item| item.canonical.as_ref())
                .map(|value| value.confidence),
            "anilist bridge below threshold; trying tvdb aliases"
        );
        let mut alias_titles = extract_tvdb_aliases(series_meta);
        alias_titles.extend(hint.alt_titles.clone());
        alias_titles = dedupe_titles(alias_titles);
        if !alias_titles.is_empty() {
            hint.alt_titles = alias_titles;
            let alias_candidates = match anilist.identify(&hint).await {
                Ok(items) => items,
                Err(err) => {
                    tracing::warn!("tvdb anime bridge alias lookup failed: {err}");
                    Vec::new()
                }
            };
            let alias_canonical = scorer.score(&hint, &alias_candidates);
            bridge_results.push(ClassifiedHint {
                hint: hint.clone(),
                canonical: alias_canonical,
            });
        }
    }

    let Some(selection) = select_best_classification(bridge_results) else {
        tracing::debug!(tvdb_id = %tvdb_id, "anilist bridge produced no hints");
        return Ok(result);
    };
    let hint = selection.hint;
    let canonical = selection.canonical;
    let hypotheses = selection.hypotheses;
    let decision = tvdb_anime_bridge_disposition(
        canonical.as_ref(),
        selection.winner_margin,
        seed.season_number,
    );

    if let Some(canonical) = canonical.as_ref() {
        tracing::debug!(
            tvdb_id = %tvdb_id,
            anilist_id = ?canonical.ids.anilist,
            confidence = canonical.confidence,
            runner_up_confidence = ?selection.runner_up_confidence,
            winner_margin = ?selection.winner_margin,
            disposition = %decision.as_db_value(),
            "anilist bridge result"
        );
    } else {
        tracing::debug!(tvdb_id = %tvdb_id, "anilist bridge produced no candidates");
    }

    if decision.is_applied() {
        if let Some(canonical) = canonical.as_ref() {
            if let Some(anilist_id) = canonical.ids.anilist.as_ref() {
                let season_seed = SeasonAnilistSeed {
                    anilist_id: anilist_id.clone(),
                    confidence: canonical.confidence,
                    causal_paths: seed.hint.source_path.clone().into_iter().collect(),
                };
                result
                    .season_anilist_ids
                    .insert(seed.season_number, season_seed);
            }
            if merged_ids.anilist.is_none() {
                let mapped = classifier_ids_to_server(&canonical.ids, MediaType::Anime);
                *merged_ids = merge_external_ids(merged_ids, Some(mapped));
            }
        }
    }

    let (hint_json, candidates_json) = build_classification_evidence_payloads(
        &hint,
        &hypotheses,
        selection.runner_up_confidence,
        selection.winner_margin,
    )?;
    apply_tvdb_anime_bridge_outcome(
        classification_outcomes,
        seed.season_number,
        decision,
        canonical.as_ref().map(|candidate| candidate.confidence),
        hint_json,
        candidates_json,
    );

    Ok(result)
}

fn apply_tvdb_anime_bridge_outcome(
    classification_outcomes: &mut HashMap<String, ClassificationOutcome>,
    seed_season: i32,
    decision: ClassificationDisposition,
    confidence: Option<f32>,
    hint_json: Option<String>,
    candidates_json: Option<String>,
) {
    for outcome in classification_outcomes.values_mut() {
        if outcome.season_scope != Some(seed_season)
            || !outcome.disposition.is_applied()
            || outcome.bridge_protected
            || outcome.preserve_authoritative_episode_links
        {
            continue;
        }

        outcome.disposition = decision;
        outcome.retry_supersedes_applied = !decision.is_applied();
        outcome.confidence = confidence;
        outcome.hint_json = hint_json.clone();
        outcome.candidates_json = compose_tvdb_anime_bridge_evidence(
            outcome.candidates_json.as_deref(),
            candidates_json.as_deref(),
        );
        // parsed_hint/accepted_numbers remain ephemeral retry context.
        // episode_number_evidence gates them on Applied, so an unresolved
        // bridge cannot create a season, episode, or file link. A bridge never
        // promotes a file whose own deterministic classification was unresolved.
    }
}

fn compose_tvdb_anime_bridge_evidence(
    primary_json: Option<&str>,
    bridge_json: Option<&str>,
) -> Option<String> {
    let mut bridge = bridge_json
        .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    let primary = primary_json
        .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
        .map(|value| value.get("primaryClassification").cloned().unwrap_or(value));

    if let Some(object) = bridge.as_object_mut() {
        object.insert(
            "stage".to_string(),
            serde_json::Value::String("tvdb_to_anilist".to_string()),
        );
        if let Some(primary) = primary {
            object.insert("primaryClassification".to_string(), primary);
        }
        return Some(bridge.to_string());
    }

    Some(
        serde_json::json!({
            "stage": "tvdb_to_anilist",
            "bridgeEvidence": bridge,
            "primaryClassification": primary,
        })
        .to_string(),
    )
}

fn extract_tvdb_title(meta: &serde_json::Value) -> Option<String> {
    let keys = ["name", "seriesName", "series_name", "title"];
    let mut raw = None;
    for key in keys {
        if let Some(value) = meta.get(key).and_then(serde_json::Value::as_str) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                raw = Some(trimmed.to_string());
                if !contains_non_ascii(trimmed) {
                    return raw;
                }
                break;
            }
        }
    }

    if let Some(translation) = extract_tvdb_translation(meta, &["eng", "en"]) {
        if !contains_non_ascii(&translation) {
            return Some(translation);
        }
    }

    for alias in extract_tvdb_aliases(meta) {
        let trimmed = alias.trim();
        if !trimmed.is_empty() && !contains_non_ascii(trimmed) {
            return Some(trimmed.to_string());
        }
    }

    raw.filter(|value| !contains_non_ascii(value))
}

fn extract_tvdb_translation(meta: &serde_json::Value, langs: &[&str]) -> Option<String> {
    for key in ["nameTranslations", "translations"] {
        if let Some(obj) = meta.get(key).and_then(serde_json::Value::as_object) {
            for lang in langs {
                if let Some(value) = obj.get(*lang).and_then(serde_json::Value::as_str) {
                    let trimmed = value.trim();
                    if !trimmed.is_empty() && !looks_like_language_code(trimmed) {
                        return Some(trimmed.to_string());
                    }
                }
            }
            for value in obj.values() {
                if let Some(text) = value.as_str() {
                    let trimmed = text.trim();
                    if !trimmed.is_empty() && !looks_like_language_code(trimmed) {
                        return Some(trimmed.to_string());
                    }
                }
            }
        } else if let Some(array) = meta.get(key).and_then(serde_json::Value::as_array) {
            for lang in langs {
                if let Some(text) = extract_translation_from_array(array, lang) {
                    return Some(text);
                }
            }
        }
    }
    None
}

fn extract_tvdb_aliases(meta: &serde_json::Value) -> Vec<String> {
    let mut aliases = Vec::new();
    if let Some(eng) = extract_tvdb_translation(meta, &["eng", "en"]) {
        aliases.push(eng);
    }
    for key in ["aliases", "aka", "nameTranslations", "translations"] {
        if let Some(values) = meta.get(key).and_then(serde_json::Value::as_array) {
            for entry in values {
                if let Some(value) = extract_tvdb_alias_text(entry) {
                    if !looks_like_language_code(&value) {
                        aliases.push(value);
                    }
                }
            }
        } else if let Some(obj) = meta.get(key).and_then(serde_json::Value::as_object) {
            for value in obj.values() {
                if let Some(text) = value.as_str() {
                    if !looks_like_language_code(text) {
                        aliases.push(text.to_string());
                    }
                }
            }
        }
    }
    dedupe_titles(aliases)
}

fn extract_tvdb_alias_text(value: &serde_json::Value) -> Option<String> {
    if let Some(text) = value.as_str() {
        return Some(text.to_string());
    }
    let obj = value.as_object()?;
    for key in ["name", "title", "alias", "text", "translation"] {
        if let Some(text) = obj.get(key).and_then(serde_json::Value::as_str) {
            return Some(text.to_string());
        }
    }
    None
}

fn extract_translation_from_array(values: &[serde_json::Value], lang: &str) -> Option<String> {
    for entry in values {
        if let Some(obj) = entry.as_object() {
            let language = obj
                .get("language")
                .or_else(|| obj.get("languageCode"))
                .or_else(|| obj.get("lang"))
                .and_then(serde_json::Value::as_str);
            if language.map(|value| value.eq_ignore_ascii_case(lang)) != Some(true) {
                continue;
            }
            if let Some(text) = extract_tvdb_alias_text(entry) {
                let trimmed = text.trim();
                if !trimmed.is_empty() && !looks_like_language_code(trimmed) {
                    return Some(trimmed.to_string());
                }
            }
        }
    }
    None
}

fn contains_non_ascii(value: &str) -> bool {
    value.chars().any(|c| !c.is_ascii())
}

fn looks_like_language_code(value: &str) -> bool {
    let trimmed = value.trim();
    let len = trimmed.len();
    if !(len == 2 || len == 3) {
        return false;
    }
    trimmed.chars().all(|c| c.is_ascii_lowercase())
}

fn extract_tvdb_genres(meta: &serde_json::Value) -> Vec<String> {
    let mut genres = Vec::new();
    if let Some(values) = meta.get("genres").and_then(serde_json::Value::as_array) {
        for entry in values {
            if let Some(value) = entry.as_str() {
                genres.push(value.to_string());
            } else if let Some(value) = entry
                .get("name")
                .or_else(|| entry.get("genre"))
                .and_then(serde_json::Value::as_str)
            {
                genres.push(value.to_string());
            }
        }
    }
    if genres.is_empty() {
        if let Some(value) = meta.get("genre").and_then(serde_json::Value::as_str) {
            genres.push(value.to_string());
        }
    }
    genres
}

fn extract_tvdb_year(meta: &serde_json::Value) -> Option<i32> {
    if let Some(year) = meta
        .get("year")
        .or_else(|| meta.get("releaseYear"))
        .and_then(serde_json::Value::as_i64)
    {
        return Some(year as i32);
    }
    if let Some(year) = meta
        .get("year")
        .or_else(|| meta.get("releaseYear"))
        .and_then(serde_json::Value::as_str)
    {
        if let Some(parsed) = parse_year_str(year) {
            return Some(parsed);
        }
    }
    let date_keys = ["firstAired", "first_air_date", "startDate", "premiereDate"];
    for key in date_keys {
        if let Some(value) = meta.get(key).and_then(serde_json::Value::as_str) {
            if let Some(year) = parse_year_str(value) {
                return Some(year);
            }
        }
    }
    None
}

fn parse_year_str(value: &str) -> Option<i32> {
    let trimmed = value.trim();
    let prefix = trimmed.get(0..4)?;
    if prefix.chars().all(|c| c.is_ascii_digit()) {
        return prefix.parse::<i32>().ok();
    }
    None
}

fn extract_tvdb_country(meta: &serde_json::Value) -> Option<String> {
    let keys = [
        "country",
        "originalCountry",
        "original_country",
        "primaryCountry",
    ];
    for key in keys {
        if let Some(value) = meta.get(key).and_then(serde_json::Value::as_str) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn tvdb_indicates_anime(meta: &serde_json::Value) -> bool {
    if let Some(country) = extract_tvdb_country(meta) {
        let lower = country.to_ascii_lowercase();
        if lower == "jpn" {
            return true;
        }
    }
    let genres = extract_tvdb_genres(meta);
    let mut has_animation = false;
    for genre in genres {
        let lower = genre.to_ascii_lowercase();
        if lower.contains("anime") {
            return true;
        }
        if lower.contains("animation") {
            has_animation = true;
        }
    }
    if has_animation {
        if let Some(country) = extract_tvdb_country(meta) {
            let lower = country.to_ascii_lowercase();
            if lower.contains("japan") || lower == "jp" {
                return true;
            }
        }
    }
    false
}

fn dedupe_titles(values: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        let key = normalized_title_key(trimmed);
        if seen.insert(key) {
            out.push(trimmed.to_string());
        }
    }
    out
}

#[derive(Debug, Clone)]
struct ClassificationSelection {
    hint: elixir_classifier::hint::ClassificationHint,
    canonical: Option<ClassifierCanonicalMatch>,
    runner_up_confidence: Option<f32>,
    winner_margin: Option<f32>,
    hypotheses: Vec<serde_json::Value>,
}

fn select_best_classification(results: Vec<ClassifiedHint>) -> Option<ClassificationSelection> {
    if results.is_empty() {
        tracing::trace!("classifier selected no hint");
        return None;
    }

    let mut ranked: Vec<(f32, usize, Option<usize>, String)> = Vec::new();
    for (hint_index, item) in results.iter().enumerate() {
        let Some(canonical) = item.canonical.as_ref() else {
            continue;
        };
        if canonical.considered.is_empty() {
            if canonical.confidence.is_finite() {
                ranked.push((
                    canonical.confidence,
                    hint_index,
                    None,
                    semantic_hypothesis_key(
                        canonical.kind.as_str(),
                        &canonical.ids,
                        canonical.chosen_provider,
                        &item.hint.title,
                        item.hint.year,
                        canonical.season.or(item.hint.season),
                        canonical.episode.or(item.hint.episode),
                        canonical.absolute_episode.or(item.hint.absolute_episode),
                    ),
                ));
            }
            continue;
        }
        for (candidate_index, candidate) in canonical.considered.iter().enumerate() {
            let score =
                if candidate_index == 0 && candidate.score == 0.0 && canonical.confidence > 0.0 {
                    canonical.confidence
                } else {
                    candidate.score
                };
            if score.is_finite() {
                ranked.push((
                    score,
                    hint_index,
                    Some(candidate_index),
                    semantic_hypothesis_key(
                        candidate.kind.as_str(),
                        &candidate.ids,
                        candidate.provider,
                        &candidate.title,
                        candidate.year,
                        candidate.season.or(item.hint.season),
                        candidate.episode.or(item.hint.episode),
                        candidate.absolute_episode.or(item.hint.absolute_episode),
                    ),
                ));
            }
        }
    }
    ranked.sort_by(|left, right| {
        right
            .0
            .total_cmp(&left.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
            .then_with(|| left.3.cmp(&right.3))
    });

    let hypotheses = ranked
        .iter()
        .map(|(score, hint_index, candidate_index, semantic_key)| {
            let item = &results[*hint_index];
            let candidate = candidate_index.and_then(|index| {
                item.canonical
                    .as_ref()
                    .and_then(|canonical| canonical.considered.get(index))
            });
            serde_json::json!({
                "hintIndex": hint_index,
                "candidateIndex": candidate_index,
                "hint": item.hint,
                "candidate": candidate,
                "score": score,
                "semanticKey": semantic_key,
            })
        })
        .collect();
    let mut distinct_keys = HashSet::new();
    let distinct_ranked: Vec<_> = ranked
        .iter()
        .filter(|entry| distinct_keys.insert(entry.3.clone()))
        .collect();
    let winner_hint_index = distinct_ranked.first().map(|entry| entry.1).unwrap_or(0);
    let winner_confidence = distinct_ranked.first().map(|entry| entry.0);
    let runner_up_confidence = distinct_ranked.get(1).map(|entry| entry.0);
    let winner_margin = winner_confidence
        .zip(runner_up_confidence)
        .map(|(winner, runner_up)| (winner - runner_up).max(0.0));

    let item = results.get(winner_hint_index)?.clone();
    let selection = ClassificationSelection {
        hint: item.hint,
        canonical: item.canonical,
        runner_up_confidence,
        winner_margin,
        hypotheses,
    };
    {
        let hint = &selection.hint;
        let canonical = &selection.canonical;
        tracing::trace!(
            hint_type = ?hint.library_type,
            hint_title = %hint.title,
            confidence = canonical.as_ref().map(|c| c.confidence),
            chosen_provider = canonical.as_ref().map(|c| c.chosen_provider),
            runner_up_confidence = ?selection.runner_up_confidence,
            winner_margin = ?selection.winner_margin,
            "classifier selected best hint"
        );
    }
    Some(selection)
}

#[allow(clippy::too_many_arguments)]
fn semantic_hypothesis_key(
    kind: &str,
    ids: &ClassifierExternalIds,
    provider: &str,
    title: &str,
    year: Option<i32>,
    season: Option<i32>,
    episode: Option<i32>,
    absolute_episode: Option<i32>,
) -> String {
    let stable_identity = [
        ("anilist", ids.anilist.as_deref()),
        ("tvdb_series", ids.tvdb_series.as_deref()),
        ("tvdb_movie", ids.tvdb_movie.as_deref()),
        ("imdb", ids.imdb.as_deref()),
        ("tmdb", ids.tmdb.as_deref()),
        ("anidb", ids.anidb.as_deref()),
        ("mal", ids.mal.as_deref()),
        ("kitsu", ids.kitsu.as_deref()),
    ]
    .into_iter()
    .find_map(|(name, value)| value.map(|value| format!("{name}:{value}")))
    .unwrap_or_else(|| {
        let normalized_title = normalized_title_key(title);
        format!("{provider}:{normalized_title}:{}", year.unwrap_or_default())
    });

    format!(
        "{kind}|{stable_identity}|s{}|e{}|a{}",
        season
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        episode
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        absolute_episode
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
    )
}

fn normalized_title_key(value: &str) -> String {
    value
        .nfkc()
        .flat_map(char::to_lowercase)
        .filter(|character| character.is_alphanumeric())
        .collect()
}

fn build_classifier_pipeline(classifier_config: Option<&ClassifierConfig>) -> ClassifierPipeline {
    let config = classifier_config.cloned().unwrap_or_default();
    let tvdb_base_url = config.tvdb_base_url.clone();
    let tvdb_api_key = config.tvdb_api_key.clone();
    let tvdb_identifier = TvdbIdentifier::new(
        tvdb_base_url.clone(),
        tvdb_api_key.clone(),
        config.request_timeout_seconds,
    );
    let tvdb_linker = TvdbLinker::new(tvdb_base_url, tvdb_api_key, config.request_timeout_seconds);
    ClassifierPipeline::new()
        .register_hint_parser(Arc::new(GeneralParser::default()))
        .register_hint_parser(Arc::new(IdExtractorParser::default()))
        .register_hint_parser(Arc::new(FolderContextParser::default()))
        .register_hint_parser(Arc::new(AnimeParserAdapter::default()))
        .register_identifier_provider(Arc::new(tvdb_identifier))
        .register_identifier_provider(Arc::new(AniListIdentifier::default()))
        .register_identifier_provider(Arc::new(CinemetaIdentifier::default()))
        .register_linker(Arc::new(tvdb_linker))
        .register_linker(Arc::new(AniZipLinker::default()))
}

fn build_classifier_input(
    file: &AggregatedFile,
    library_type: MediaType,
    ids: &ExternalIds,
) -> ClassifierFileInput {
    let path = file.descriptor.path.clone();
    let file_name = Path::new(&path)
        .file_name()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string());
    let parent_name = Path::new(&path)
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|s| s.to_str())
        .map(|s| s.to_string());

    let mut input = ClassifierFileInput::new(path);
    input.file_name = file_name;
    input.parent_name = parent_name;
    input.size_bytes = file.descriptor.size_bytes;
    input.duration_seconds = None;
    input.season = file.season;
    input.episode = file.episode;
    input.absolute_episode = file.absolute_episode;
    input.embedded_ids = classifier_ids_from_server(ids, library_type);
    input.library_type_hint = Some(classifier_library_type(library_type));
    input
}

fn classifier_library_type(media_type: MediaType) -> ClassifierLibraryType {
    match media_type {
        MediaType::Movie => ClassifierLibraryType::Movie,
        MediaType::Series => ClassifierLibraryType::Series,
        MediaType::Anime => ClassifierLibraryType::Anime,
    }
}

fn classifier_ids_from_server(ids: &ExternalIds, media_type: MediaType) -> ClassifierExternalIds {
    let (tvdb_series, tvdb_movie) = match media_type {
        MediaType::Movie => (None, ids.tvdb_movie.clone().or_else(|| ids.tvdb.clone())),
        _ => (ids.tvdb_series.clone().or_else(|| ids.tvdb.clone()), None),
    };
    ClassifierExternalIds {
        imdb: ids.imdb.clone(),
        tmdb: ids.tmdb.clone(),
        tvdb_series,
        tvdb_movie,
        anilist: ids.anilist.clone(),
        anidb: ids.anidb.clone(),
        mal: ids.mal.clone(),
        kitsu: ids.kitsu.clone(),
    }
}

fn classifier_ids_to_server(ids: &ClassifierExternalIds, media_type: MediaType) -> ExternalIds {
    let (tvdb_series, tvdb_movie) = match media_type {
        MediaType::Movie => (
            None,
            ids.tvdb_movie.clone().or_else(|| ids.tvdb_series.clone()),
        ),
        _ => (
            ids.tvdb_series.clone().or_else(|| ids.tvdb_movie.clone()),
            None,
        ),
    };
    ExternalIds {
        imdb: ids.imdb.clone(),
        tmdb: ids.tmdb.clone(),
        tvdb: None,
        tvdb_series,
        tvdb_movie,
        anilist: ids.anilist.clone(),
        anidb: ids.anidb.clone(),
        mal: ids.mal.clone(),
        kitsu: ids.kitsu.clone(),
    }
}

fn has_strong_ids(media_type: MediaType, ids: &ExternalIds) -> bool {
    match media_type {
        MediaType::Movie => ids.imdb.is_some() || ids.tmdb.is_some() || ids.tvdb_movie.is_some(),
        MediaType::Series => ids.imdb.is_some() || ids.tvdb_series.is_some(),
        MediaType::Anime => ids.anilist.is_some(),
    }
}

fn library_type_string(media_type: MediaType) -> &'static str {
    match media_type {
        MediaType::Movie => "movie",
        MediaType::Series => "series",
        MediaType::Anime => "anime",
    }
}

fn strip_anime_ids(ids: &ExternalIds) -> ExternalIds {
    if ids.anidb.is_some() || ids.mal.is_some() || ids.kitsu.is_some() {
        tracing::trace!(
            anidb = ?ids.anidb,
            mal = ?ids.mal,
            kitsu = ?ids.kitsu,
            "stripping secondary anime ids from series-level storage"
        );
    }
    let mut cleaned = ids.clone();
    cleaned.anidb = None;
    cleaned.mal = None;
    cleaned.kitsu = None;
    cleaned
}

async fn lookup_override_for_path(
    pool: &AnyPool,
    library_type: &str,
    path: &str,
    cache: &mut HashMap<String, Option<ExternalIds>>,
) -> Result<Option<ExternalIds>> {
    let key = match derive_override_key(library_type, path) {
        Some(key) => key,
        None => return Ok(None),
    };
    if let Some(cached) = cache.get(&key) {
        return Ok(cached.clone());
    }

    let row = sqlx::query(
        "SELECT imdb_id, anilist_id, tvdb_id FROM classifier_overrides WHERE library_type = $1 AND normalized_key = $2 LIMIT 1",
    )
    .bind(library_type)
    .bind(&key)
    .fetch_optional(pool)
    .await?;

    let ids = row.map(|row| {
        let imdb: Option<String> = row.try_get("imdb_id").ok();
        let anilist: Option<String> = row.try_get("anilist_id").ok();
        let tvdb: Option<String> = row.try_get("tvdb_id").ok();
        let (tvdb_series, tvdb_movie) = match library_type {
            "movie" => (None, tvdb),
            _ => (tvdb, None),
        };
        ExternalIds {
            imdb,
            tmdb: None,
            tvdb: None,
            tvdb_series,
            tvdb_movie,
            anilist,
            anidb: None,
            mal: None,
            kitsu: None,
        }
    });

    cache.insert(key, ids.clone());
    Ok(ids)
}

async fn persist_classification_outcome(
    pool: &AnyPool,
    media_file_id: Uuid,
    outcome: &ClassificationOutcome,
) -> Result<()> {
    match outcome.disposition {
        ClassificationDisposition::Applied => {
            mark_classification_applied(pool, media_file_id, outcome).await?
        }
        ClassificationDisposition::Unresolved => {
            upsert_unresolved_classification(pool, media_file_id, outcome).await?;
        }
    }
    Ok(())
}

async fn mark_classification_applied(
    pool: &AnyPool,
    media_file_id: Uuid,
    outcome: &ClassificationOutcome,
) -> Result<()> {
    let mut identity_evidence = applied_classification_identity_evidence(outcome)?;
    if identity_evidence.is_some() {
        let existing: Option<(Option<String>, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT hint_json, candidates_json, applied_identity_evidence_json \
             FROM classifier_resolution_state \
             WHERE media_file_id = $1 AND disposition = 'applied' LIMIT 1",
        )
        .bind(media_file_id.to_string())
        .fetch_optional(pool)
        .await?;
        if let Some(existing) = existing.as_ref() {
            identity_evidence = reconcile_idempotent_applied_identity_evidence(
                outcome,
                existing,
                identity_evidence,
            )?;
        }
    }
    let identity_version = identity_evidence
        .as_ref()
        .map(|_| APPLIED_CLASSIFICATION_IDENTITY_EVIDENCE_SCHEMA_VERSION)
        .unwrap_or_default();
    let anime_match_assist = classification_anime_match_assist(outcome);
    sqlx::query::<sqlx::Any>(
        "INSERT INTO classifier_resolution_state (media_file_id, disposition, confidence, \
         hint_json, candidates_json, applied_identity_version, \
         applied_identity_evidence_json, anime_match_assist_json, created_at, updated_at) \
         VALUES ($1, 'applied', $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) \
         ON CONFLICT(media_file_id) DO UPDATE SET \
         disposition = 'applied', \
         confidence = COALESCE(excluded.confidence, classifier_resolution_state.confidence), \
         hint_json = COALESCE(excluded.hint_json, classifier_resolution_state.hint_json), \
         candidates_json = COALESCE(\
             excluded.candidates_json, classifier_resolution_state.candidates_json\
         ), applied_identity_version = CASE \
             WHEN excluded.applied_identity_version > 0 \
             THEN excluded.applied_identity_version \
             ELSE classifier_resolution_state.applied_identity_version END, \
         applied_identity_evidence_json = COALESCE( \
             excluded.applied_identity_evidence_json, \
             classifier_resolution_state.applied_identity_evidence_json \
         ), anime_match_assist_json = COALESCE( \
             excluded.anime_match_assist_json, \
             classifier_resolution_state.anime_match_assist_json \
         ), \
         updated_at = CURRENT_TIMESTAMP",
    )
    .bind(media_file_id.to_string())
    .bind(outcome.confidence)
    .bind(outcome.hint_json.as_ref())
    .bind(outcome.candidates_json.as_ref())
    .bind(identity_version)
    .bind(identity_evidence.as_ref())
    .bind(anime_match_assist.as_ref())
    .execute(pool)
    .await?;
    Ok(())
}

async fn upsert_unresolved_classification(
    pool: &AnyPool,
    media_file_id: Uuid,
    outcome: &ClassificationOutcome,
) -> Result<()> {
    let anime_match_assist = classification_anime_match_assist(outcome);
    sqlx::query::<sqlx::Any>(
        "INSERT INTO classifier_resolution_state (media_file_id, disposition, confidence, \
         hint_json, candidates_json, applied_identity_version, \
         applied_identity_evidence_json, anime_match_assist_json, created_at, updated_at) \
         VALUES ($1, 'unresolved', $2, $3, $4, 0, NULL, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) \
         ON CONFLICT(media_file_id) DO UPDATE SET \
         disposition = 'unresolved', confidence = excluded.confidence, \
         hint_json = excluded.hint_json, candidates_json = excluded.candidates_json, \
         applied_identity_version = 0, applied_identity_evidence_json = NULL, \
         anime_match_assist_json = excluded.anime_match_assist_json, \
         updated_at = CURRENT_TIMESTAMP \
         WHERE (classifier_resolution_state.disposition != 'applied' OR $6 = TRUE) \
           AND NOT EXISTS (SELECT 1 FROM library_anime_repairs lar \
                           WHERE lar.media_file_id = $1 \
                             AND lar.repair_version = $7 \
                             AND lar.status = 'completed')",
    )
    .bind(media_file_id.to_string())
    .bind(outcome.confidence)
    .bind(outcome.hint_json.as_ref())
    .bind(outcome.candidates_json.as_ref())
    .bind(anime_match_assist.as_ref())
    .bind(outcome.retry_supersedes_applied)
    .bind(ANIME_LIBRARY_REPAIR_VERSION)
    .execute(pool)
    .await?;

    Ok(())
}

fn classification_anime_match_assist(outcome: &ClassificationOutcome) -> Option<String> {
    let value = outcome
        .candidates_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())?;
    let assist = value.get("animeMatchAssist")?.clone();
    serde_json::to_string(&serde_json::json!({ "animeMatchAssist": assist })).ok()
}

fn persisted_applied_result_matches(
    outcome: &ClassificationOutcome,
    existing: &(Option<String>, Option<String>, Option<String>),
) -> bool {
    let accepted_numbers = outcome
        .accepted_numbers
        .map(|numbers| {
            serde_json::json!({
                "seasonNumber": numbers.season,
                "episodeNumber": numbers.episode,
                "absoluteEpisodeNumber": numbers.absolute_episode,
            })
        })
        .unwrap_or(serde_json::Value::Null);
    let existing_numbers_match = existing
        .2
        .as_deref()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .and_then(|value| value.get("acceptedNumbers").cloned())
        == Some(accepted_numbers);
    existing_numbers_match
        && optional_json_payloads_equal(existing.0.as_deref(), outcome.hint_json.as_deref())
        && optional_json_payloads_equal(existing.1.as_deref(), outcome.candidates_json.as_deref())
}

fn optional_json_payloads_equal(left: Option<&str>, right: Option<&str>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => {
            match (
                serde_json::from_str::<serde_json::Value>(left),
                serde_json::from_str::<serde_json::Value>(right),
            ) {
                (Ok(left), Ok(right)) => left == right,
                _ => left == right,
            }
        }
        _ => false,
    }
}

fn reconcile_idempotent_applied_identity_evidence(
    outcome: &ClassificationOutcome,
    existing: &(Option<String>, Option<String>, Option<String>),
    new_evidence: Option<String>,
) -> Result<Option<String>> {
    let same_applied_result = persisted_applied_result_matches(outcome, existing);
    let Some(existing_evidence) = existing.2.as_deref() else {
        return Ok(new_evidence);
    };
    if same_applied_result
        && outcome.applied_identity_rows == AppliedClassificationIdentityRows::default()
    {
        // A forced/idempotent reclassification cannot re-insert rows that
        // already exist. Keep the original exact ownership envelope instead
        // of replacing it with an empty one.
        return Ok(None);
    }
    let Some(new_evidence) = new_evidence else {
        return Ok(None);
    };
    let existing_value = serde_json::from_str::<serde_json::Value>(existing_evidence)?;
    if existing_value
        .get("schemaVersion")
        .and_then(serde_json::Value::as_i64)
        != Some(i64::from(
            APPLIED_CLASSIFICATION_IDENTITY_EVIDENCE_SCHEMA_VERSION,
        ))
    {
        // Legacy evidence cannot be unioned without inventing exact row
        // ownership. Preserve it byte-for-byte for an idempotent replay, but
        // allow a genuinely new Applied result to start an exact v2 envelope.
        return if same_applied_result {
            Ok(None)
        } else {
            Ok(Some(new_evidence))
        };
    }
    let mut merged = serde_json::from_str::<serde_json::Value>(&new_evidence)?;
    for level in ["series", "seasons", "episodes"] {
        let mut exact_rows = BTreeMap::new();
        for value in [
            existing_value
                .get("causalIdentityRows")
                .and_then(|rows| rows.get(level)),
            merged
                .get("causalIdentityRows")
                .and_then(|rows| rows.get(level)),
        ]
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_array)
        .flatten()
        {
            exact_rows.insert(serde_json::to_string(value)?, value.clone());
        }
        merged["causalIdentityRows"][level] =
            serde_json::Value::Array(exact_rows.into_values().collect());
    }
    Ok(Some(serde_json::to_string(&merged)?))
}

fn applied_classification_identity_evidence(
    outcome: &ClassificationOutcome,
) -> Result<Option<String>> {
    if outcome.hint_json.is_none() && outcome.candidates_json.is_none() {
        // Reusing a persisted canonical link can repeat accepted numbering
        // without producing new classifier/model evidence. Preserve the
        // original causal envelope instead of relabeling it deterministic on
        // every rescan.
        return Ok(None);
    }
    let hint = outcome
        .hint_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok());
    let candidates = outcome
        .candidates_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok());
    let assist_source = candidates
        .as_ref()
        .and_then(|value| value.get("animeMatchAssist"))
        .and_then(|value| value.get("source"))
        .and_then(serde_json::Value::as_str);
    let accepted = outcome.accepted_numbers.map(|numbers| {
        serde_json::json!({
            "seasonNumber": numbers.season,
            "episodeNumber": numbers.episode,
            "absoluteEpisodeNumber": numbers.absolute_episode,
        })
    });
    Ok(Some(serde_json::to_string(&serde_json::json!({
        "schemaVersion": APPLIED_CLASSIFICATION_IDENTITY_EVIDENCE_SCHEMA_VERSION,
        "origin": assist_source.unwrap_or("deterministic_classifier"),
        "acceptedNumbers": accepted,
        "causalIdentityRows": {
            "series": outcome.applied_identity_rows.series.iter().map(|row| {
                serde_json::json!({
                    "provider": row.provider,
                    "externalId": row.external_id,
                    "source": row.source,
                })
            }).collect::<Vec<_>>(),
            "seasons": outcome.applied_identity_rows.seasons.iter().map(|row| {
                serde_json::json!({
                    "seasonNumber": row.season_number,
                    "provider": row.provider,
                    "externalId": row.external_id,
                    "source": row.source,
                })
            }).collect::<Vec<_>>(),
            "episodes": outcome.applied_identity_rows.episodes.iter().map(|row| {
                serde_json::json!({
                    "episodeId": row.episode_id,
                    "provider": row.provider,
                    "externalId": row.external_id,
                    "source": row.source,
                })
            }).collect::<Vec<_>>(),
        },
        "hint": hint,
        "candidates": candidates,
    }))?))
}

#[derive(Debug, Default)]
struct AppliedClassifierIdentityClaim {
    series_ids: BTreeSet<(String, String)>,
    season_number: Option<i32>,
}

fn applied_classifier_identity_claim(
    outcome: &ClassificationOutcome,
) -> Option<AppliedClassifierIdentityClaim> {
    if !outcome.disposition.is_applied() {
        return None;
    }
    classifier_identity_claim(outcome)
}

fn classifier_identity_claim(
    outcome: &ClassificationOutcome,
) -> Option<AppliedClassifierIdentityClaim> {
    let candidates = outcome
        .candidates_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())?;
    let mut claim = AppliedClassifierIdentityClaim {
        season_number: outcome
            .accepted_numbers
            .and_then(|numbers| numbers.season)
            .or(outcome.season_scope),
        ..Default::default()
    };
    let mut found_winner = false;
    for evidence in [Some(&candidates), candidates.get("primaryClassification")]
        .into_iter()
        .flatten()
    {
        // Hypotheses are persisted in winner-first order. Looking only at each
        // stage's concrete winner is deliberate: a runner-up identity must
        // never become deletable merely because it appeared in diagnostics.
        let Some(candidate) = evidence
            .get("hypotheses")
            .and_then(serde_json::Value::as_array)
            .and_then(|hypotheses| hypotheses.first())
            .and_then(|hypothesis| hypothesis.get("candidate"))
            .and_then(serde_json::Value::as_object)
        else {
            continue;
        };
        found_winner = true;
        if claim.season_number.is_none() {
            claim.season_number = candidate
                .get("season")
                .and_then(serde_json::Value::as_i64)
                .and_then(|number| i32::try_from(number).ok());
        }
        let Some(ids) = candidate.get("ids").and_then(serde_json::Value::as_object) else {
            continue;
        };
        for (json_key, provider) in [
            ("imdb", "imdb"),
            ("tmdb", "tmdb"),
            ("tvdbSeries", "tvdb"),
            ("tvdb_series", "tvdb"),
            ("tvdb", "tvdb"),
            ("anilist", "anilist"),
            ("anidb", "anidb"),
            ("mal", "mal"),
            ("kitsu", "kitsu"),
        ] {
            let Some(external_id) = ids
                .get(json_key)
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
            else {
                continue;
            };
            claim
                .series_ids
                .insert((provider.to_string(), external_id.to_string()));
        }
    }
    found_winner.then_some(claim)
}

fn external_ids_contain_persisted_series_row(
    ids: &ExternalIds,
    row: &PersistedExternalIdentityRow,
) -> bool {
    let matches = |value: Option<&String>| {
        value.is_some_and(|value| value.trim().eq_ignore_ascii_case(row.external_id.trim()))
    };
    match row.provider.as_str() {
        "imdb" => matches(ids.imdb.as_ref()),
        "tmdb" => matches(ids.tmdb.as_ref()),
        "tvdb" => matches(ids.tvdb_series.as_ref()) || matches(ids.tvdb.as_ref()),
        "anilist" => matches(ids.anilist.as_ref()),
        "anidb" => matches(ids.anidb.as_ref()),
        "mal" => matches(ids.mal.as_ref()),
        "kitsu" => matches(ids.kitsu.as_ref()),
        _ => false,
    }
}

#[derive(Debug, Clone, Copy)]
enum AppliedIdentityAttributionTarget {
    Series { claimant_season: Option<i32> },
    Season { season_number: i32 },
}

fn attribute_inserted_classification_identity_rows(
    outcomes: &mut HashMap<String, ClassificationOutcome>,
    target: AppliedIdentityAttributionTarget,
    inserted_rows: &[PersistedExternalIdentityRow],
    causal_paths: Option<&BTreeSet<String>>,
) {
    for inserted in inserted_rows
        .iter()
        .filter(|row| matches!(row.source.as_str(), "classifier" | "anilist_chain"))
    {
        let owners = outcomes
            .iter()
            .filter_map(|(path, outcome)| {
                if let Some(paths) = causal_paths {
                    return (outcome.disposition.is_applied() && paths.contains(path))
                        .then(|| path.clone());
                }
                let claim = applied_classifier_identity_claim(outcome)?;
                if !claim
                    .series_ids
                    .contains(&(inserted.provider.clone(), inserted.external_id.clone()))
                {
                    return None;
                }
                let required_season = match target {
                    AppliedIdentityAttributionTarget::Series { claimant_season } => claimant_season,
                    AppliedIdentityAttributionTarget::Season { season_number } => {
                        Some(season_number)
                    }
                };
                if required_season.is_some() && claim.season_number != required_season {
                    return None;
                }
                Some(path.clone())
            })
            .take(2)
            .collect::<Vec<_>>();
        // A row claimed by multiple files is shared series context. Leaving it
        // out is essential: repairing either sibling must not delete identity
        // that may have been established by the other.
        if owners.len() != 1 {
            continue;
        }
        let Some(outcome) = outcomes.get_mut(&owners[0]) else {
            continue;
        };
        match target {
            AppliedIdentityAttributionTarget::Series { .. } => {
                outcome
                    .applied_identity_rows
                    .series
                    .insert(inserted.clone());
            }
            AppliedIdentityAttributionTarget::Season { season_number } => {
                outcome
                    .applied_identity_rows
                    .seasons
                    .insert(PersistedSeasonExternalIdentityRow {
                        season_number,
                        provider: inserted.provider.clone(),
                        external_id: inserted.external_id.clone(),
                        source: inserted.source.clone(),
                    });
            }
        }
    }
}

async fn persist_classification_outcome_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    media_file_id: Uuid,
    outcome: &ClassificationOutcome,
) -> Result<()> {
    let anime_match_assist = classification_anime_match_assist(outcome);
    match outcome.disposition {
        ClassificationDisposition::Applied => {
            let mut identity_evidence = applied_classification_identity_evidence(outcome)?;
            if identity_evidence.is_some() {
                let existing: Option<(Option<String>, Option<String>, Option<String>)> =
                    sqlx::query_as(
                        "SELECT hint_json, candidates_json, applied_identity_evidence_json \
                         FROM classifier_resolution_state \
                         WHERE media_file_id = $1 AND disposition = 'applied' LIMIT 1",
                    )
                    .bind(media_file_id.to_string())
                    .fetch_optional(&mut **transaction)
                    .await?;
                if let Some(existing) = existing.as_ref() {
                    identity_evidence = reconcile_idempotent_applied_identity_evidence(
                        outcome,
                        existing,
                        identity_evidence,
                    )?;
                }
            }
            let identity_version = identity_evidence
                .as_ref()
                .map(|_| APPLIED_CLASSIFICATION_IDENTITY_EVIDENCE_SCHEMA_VERSION)
                .unwrap_or_default();
            sqlx::query::<sqlx::Any>(
                "INSERT INTO classifier_resolution_state \
                 (media_file_id, disposition, confidence, hint_json, candidates_json, \
                  applied_identity_version, applied_identity_evidence_json, \
                  anime_match_assist_json, created_at, updated_at) \
                 VALUES ($1, 'applied', $2, $3, $4, $5, $6, $7, \
                         CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) \
                 ON CONFLICT(media_file_id) DO UPDATE SET disposition = 'applied', \
                 confidence = COALESCE( \
                     excluded.confidence, classifier_resolution_state.confidence \
                 ), hint_json = COALESCE( \
                     excluded.hint_json, classifier_resolution_state.hint_json \
                 ), candidates_json = COALESCE( \
                     excluded.candidates_json, classifier_resolution_state.candidates_json \
                 ), applied_identity_version = CASE \
                     WHEN excluded.applied_identity_version > 0 \
                     THEN excluded.applied_identity_version \
                     ELSE classifier_resolution_state.applied_identity_version END, \
                 applied_identity_evidence_json = COALESCE( \
                     excluded.applied_identity_evidence_json, \
                     classifier_resolution_state.applied_identity_evidence_json \
                 ), anime_match_assist_json = COALESCE( \
                     excluded.anime_match_assist_json, \
                     classifier_resolution_state.anime_match_assist_json \
                 ), \
                 updated_at = CURRENT_TIMESTAMP",
            )
            .bind(media_file_id.to_string())
            .bind(outcome.confidence)
            .bind(outcome.hint_json.as_ref())
            .bind(outcome.candidates_json.as_ref())
            .bind(identity_version)
            .bind(identity_evidence.as_ref())
            .bind(anime_match_assist.as_ref())
            .execute(&mut **transaction)
            .await?;
        }
        ClassificationDisposition::Unresolved => {
            sqlx::query::<sqlx::Any>(
                "INSERT INTO classifier_resolution_state \
                 (media_file_id, disposition, confidence, hint_json, candidates_json, \
                  applied_identity_version, applied_identity_evidence_json, \
                  anime_match_assist_json, created_at, updated_at) \
                 VALUES ($1, 'unresolved', $2, $3, $4, 0, NULL, $5, \
                         CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) \
                 ON CONFLICT(media_file_id) DO UPDATE SET disposition = 'unresolved', \
                 confidence = excluded.confidence, hint_json = excluded.hint_json, \
                 candidates_json = excluded.candidates_json, applied_identity_version = 0, \
                 applied_identity_evidence_json = NULL, \
                 anime_match_assist_json = excluded.anime_match_assist_json, \
                 updated_at = CURRENT_TIMESTAMP \
                 WHERE (classifier_resolution_state.disposition != 'applied' OR $6 = TRUE) \
                   AND NOT EXISTS (SELECT 1 FROM library_anime_repairs lar \
                                   WHERE lar.media_file_id = $1 \
                                     AND lar.repair_version = $7 \
                                     AND lar.status = 'completed')",
            )
            .bind(media_file_id.to_string())
            .bind(outcome.confidence)
            .bind(outcome.hint_json.as_ref())
            .bind(outcome.candidates_json.as_ref())
            .bind(anime_match_assist.as_ref())
            .bind(outcome.retry_supersedes_applied)
            .bind(ANIME_LIBRARY_REPAIR_VERSION)
            .execute(&mut **transaction)
            .await?;
        }
    }
    Ok(())
}

async fn should_refresh_metadata(
    pool: &AnyPool,
    identity: &MediaIdentity,
    ttl_seconds: u64,
    force: bool,
) -> Result<bool> {
    if force {
        return Ok(true);
    }

    let existing = match identity.r#type {
        MediaType::Movie => {
            let mut existing = None;
            if let Some(imdb) = identity.external_ids.imdb.as_ref() {
                existing = sqlx::query::<sqlx::Any>(
                    "SELECT metadata_json, CAST(updated_at AS TEXT) as updated_at FROM movies WHERE external_imdb = $1 LIMIT 1",
                )
                .bind(imdb)
                .fetch_optional(pool)
                .await?;
            }
            if existing.is_none() {
                if let Some(tvdb) = identity
                    .external_ids
                    .tvdb_movie
                    .as_ref()
                    .or(identity.external_ids.tvdb.as_ref())
                {
                    existing = sqlx::query::<sqlx::Any>(
                        "SELECT m.metadata_json, CAST(m.updated_at AS TEXT) as updated_at
                     FROM movies m
                     JOIN movie_external_ids mei ON mei.movie_id = m.id
                     WHERE mei.provider = 'tvdb' AND mei.external_id = $1
                     LIMIT 1",
                    )
                    .bind(tvdb)
                    .fetch_optional(pool)
                    .await?;
                }
            }
            if existing.is_none() {
                if let Some(tmdb) = identity.external_ids.tmdb.as_ref() {
                    existing = sqlx::query::<sqlx::Any>(
                    "SELECT metadata_json, CAST(updated_at AS TEXT) as updated_at FROM movies WHERE external_tmdb = $1 LIMIT 1",
                )
                .bind(tmdb)
                .fetch_optional(pool)
                    .await?;
                }
            }
            if existing.is_none() {
                existing = sqlx::query::<sqlx::Any>(
                    "SELECT metadata_json, CAST(updated_at AS TEXT) as updated_at FROM movies WHERE title = $1 AND (year = $2 OR (year IS NULL AND $3 IS NULL)) LIMIT 1",
                )
                .bind(&identity.title)
                .bind(identity.year)
                .bind(identity.year)
                .fetch_optional(pool)
                    .await?;
            }
            existing
        }
        MediaType::Series | MediaType::Anime => {
            let library_type = identity.r#type.as_str();
            if let Some(anilist) = identity.external_ids.anilist.as_ref() {
                sqlx::query::<sqlx::Any>(
                    "SELECT metadata_json, CAST(updated_at AS TEXT) as updated_at FROM series WHERE library_type = $1 AND external_anilist = $2 LIMIT 1",
                )
                .bind(library_type)
                .bind(anilist)
                .fetch_optional(pool)
                .await?
            } else if let Some(tvdb) = identity
                .external_ids
                .tvdb_series
                .as_ref()
                .or(identity.external_ids.tvdb.as_ref())
            {
                sqlx::query::<sqlx::Any>(
                    "SELECT metadata_json, CAST(updated_at AS TEXT) as updated_at FROM series WHERE library_type = $1 AND external_tvdb_series = $2 LIMIT 1",
                )
                .bind(library_type)
                .bind(tvdb)
                .fetch_optional(pool)
                .await?
            } else if let Some(imdb) = identity.external_ids.imdb.as_ref() {
                sqlx::query::<sqlx::Any>(
                    "SELECT metadata_json, CAST(updated_at AS TEXT) as updated_at FROM series WHERE library_type = $1 AND external_imdb = $2 LIMIT 1",
                )
                .bind(library_type)
                .bind(imdb)
                .fetch_optional(pool)
                .await?
            } else {
                sqlx::query::<sqlx::Any>(
                    "SELECT metadata_json, CAST(updated_at AS TEXT) as updated_at FROM series WHERE library_type = $1 AND title = $2 AND (year = $3 OR (year IS NULL AND $4 IS NULL)) LIMIT 1",
                )
                .bind(library_type)
                .bind(&identity.title)
                .bind(identity.year)
                .bind(identity.year)
                .fetch_optional(pool)
                .await?
            }
        }
    };

    if let Some(row) = existing {
        let meta: Option<String> = row.try_get("metadata_json").ok();
        if meta.is_none() {
            return Ok(true);
        }
        let updated: Option<String> = row.try_get("updated_at").ok();
        if let Some(updated) = updated {
            if let Ok(parsed) = updated.parse::<chrono::NaiveDateTime>() {
                let updated_ts = chrono::DateTime::<Utc>::from_naive_utc_and_offset(parsed, Utc);
                let age = Utc::now() - updated_ts;
                return Ok(age.num_seconds() as u64 > ttl_seconds);
            }
        }
        return Ok(false);
    }

    Ok(true)
}

pub async fn run_extension_scan(
    state: &AppState,
    force_metadata: bool,
    force_reclassify: bool,
) -> Result<()> {
    let candidates = state
        .extensions
        .scan_all_with_db(&state.db_pool, &state.settings.library.sonarr, None)
        .await?;
    let anime_matching = state.anime_inference.matching_service();
    run_full_scan_with_anime_matching(
        &state.db_pool,
        Some(&state.metadata),
        Some(&state.linkers),
        Some(&state.settings.classifier),
        Some(&state.artwork),
        &anime_matching,
        candidates,
        force_metadata,
        force_reclassify,
        state.settings.library.hash_dedupe_enabled,
    )
    .await?;
    request_anime_library_repair_after_scan(force_metadata);
    Ok(())
}

pub async fn start_periodic_scan(state: AppState, interval_seconds: u64) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_seconds));
    // Startup already performs one explicitly sequenced scan before repair.
    // Consume Tokio's immediate first tick so this worker cannot race it.
    interval.tick().await;
    loop {
        interval.tick().await;
        if let Err(err) = run_extension_scan(&state, false, false).await {
            tracing::warn!("periodic scan failed: {err}");
        }
    }
}

async fn upsert_movie(
    pool: &AnyPool,
    identity: &MediaIdentity,
    merged_ids: &ExternalIds,
    meta: Option<&MetadataResult>,
) -> Result<Uuid> {
    let mut existing = None;
    if let Some(imdb) = merged_ids.imdb.as_ref() {
        existing = sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT id FROM movies WHERE external_imdb = $1 LIMIT 1",
        )
        .bind(imdb)
        .fetch_optional(pool)
        .await?;
    }
    if existing.is_none() {
        if let Some(tvdb) = merged_ids.tvdb_movie.as_ref().or(merged_ids.tvdb.as_ref()) {
            existing = sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT movie_id FROM movie_external_ids WHERE provider = 'tvdb' AND external_id = $1 LIMIT 1",
        )
        .bind(tvdb)
        .fetch_optional(pool)
            .await?;
        }
    }
    if existing.is_none() {
        if let Some(tmdb) = merged_ids.tmdb.as_ref() {
            existing = sqlx::query_scalar::<sqlx::Any, String>(
                "SELECT id FROM movies WHERE external_tmdb = $1 LIMIT 1",
            )
            .bind(tmdb)
            .fetch_optional(pool)
            .await?;
        }
    }
    if existing.is_none() {
        existing = sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT id FROM movies WHERE title = $1 AND (year = $2 OR (year IS NULL AND $3 IS NULL)) LIMIT 1",
        )
        .bind(&identity.title)
        .bind(identity.year)
        .bind(identity.year)
        .fetch_optional(pool)
        .await?;
    }

    if let Some(id_str) = existing {
        let id = Uuid::parse_str(&id_str)?;
        let identity_lock = load_managed_identity_lock(pool, &id_str).await?;
        let mut title = identity.title.clone();
        let mut year = identity.year;
        let mut ids = merged_ids.clone();
        if let Some(lock) = identity_lock {
            title = lock.title;
            year = lock.year;
            if let Some(locked_ids) = lock.external_ids {
                ids = merge_external_ids(&locked_ids, Some(ids));
            }
        }
        sqlx::query::<sqlx::Any>(
            "UPDATE movies SET title = $1, year = $2, external_imdb = $3, external_tmdb = $4, metadata_json = COALESCE($5, metadata_json), runtime_seconds = COALESCE($6, runtime_seconds), updated_at = CURRENT_TIMESTAMP WHERE id = $7",
        )
        .bind(&title)
        .bind(year)
        .bind(ids.imdb.as_ref())
        .bind(ids.tmdb.as_ref())
        .bind(meta.and_then(|m| serde_json::to_string(&m.metadata_json).ok()))
        .bind(meta.and_then(|m| m.runtime_seconds))
        .bind(id_str)
        .execute(pool)
        .await?;
        return Ok(id);
    }

    let id = Uuid::new_v4();
    sqlx::query::<sqlx::Any>(
        "INSERT INTO movies (id, title, year, external_imdb, external_tmdb, metadata_json, runtime_seconds, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(id.to_string())
    .bind(&identity.title)
    .bind(identity.year)
    .bind(merged_ids.imdb.as_ref())
    .bind(merged_ids.tmdb.as_ref())
    .bind(meta.and_then(|m| serde_json::to_string(&m.metadata_json).ok()))
    .bind(meta.and_then(|m| m.runtime_seconds))
    .execute(pool)
    .await?;
    Ok(id)
}

async fn upsert_series(
    pool: &AnyPool,
    identity: &MediaIdentity,
    merged_ids: &ExternalIds,
    meta: Option<&MetadataResult>,
) -> Result<Uuid> {
    let mut existing = None;
    if let Some(anilist) = merged_ids.anilist.as_ref() {
        existing = sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT series_id FROM series_external_ids WHERE provider = 'anilist' AND external_id = $1 LIMIT 1",
        )
        .bind(anilist)
        .fetch_optional(pool)
        .await?;
        if existing.is_none() {
            existing = sqlx::query_scalar::<sqlx::Any, String>(
                "SELECT id FROM series WHERE external_anilist = $1 LIMIT 1",
            )
            .bind(anilist)
            .fetch_optional(pool)
            .await?;
        }
    }
    if existing.is_none() {
        if let Some(imdb) = merged_ids.imdb.as_ref() {
            existing = sqlx::query_scalar::<sqlx::Any, String>(
                "SELECT id FROM series WHERE external_imdb = $1 LIMIT 1",
            )
            .bind(imdb)
            .fetch_optional(pool)
            .await?;
        }
    }
    if existing.is_none() {
        if let Some(tvdb) = merged_ids.tvdb_series.as_ref().or(merged_ids.tvdb.as_ref()) {
            existing = sqlx::query_scalar::<sqlx::Any, String>(
                "SELECT id FROM series WHERE external_tvdb_series = $1 LIMIT 1",
            )
            .bind(tvdb)
            .fetch_optional(pool)
            .await?;
        }
    }
    if existing.is_none() {
        // Fallback: match by title, allowing for loose type/year matching
        let rows = sqlx::query("SELECT id, year FROM series WHERE title = $1")
            .bind(&identity.title)
            .fetch_all(pool)
            .await?;

        let mut best_match: Option<String> = None;
        for row in rows {
            let db_year: Option<i32> = row.try_get::<i64, _>("year").ok().map(|v| v as i32);

            let year_match = match (identity.year, db_year) {
                (Some(y1), Some(y2)) => y1 == y2,
                _ => true, // If either is missing, assume match
            };

            if year_match {
                best_match = Some(row.get::<String, _>("id"));
                break;
            }
        }
        existing = best_match;
    }

    if let Some(id_str) = existing {
        let id = Uuid::parse_str(&id_str)?;
        let identity_lock = load_managed_identity_lock(pool, &id_str).await?;
        let has_identity_lock = identity_lock.is_some();
        let mut title = identity.title.clone();
        let mut year = identity.year;
        let mut media_type = identity.r#type;
        let mut ids = merged_ids.clone();
        if let Some(lock) = identity_lock {
            title = lock.title;
            year = lock.year;
            if matches!(lock.media_type, MediaType::Series | MediaType::Anime) {
                media_type = lock.media_type;
            }
            if let Some(locked_ids) = lock.external_ids {
                ids = merge_external_ids(&locked_ids, Some(ids));
            }
        }
        sqlx::query::<sqlx::Any>(
            "UPDATE series SET \
             title = CASE WHEN $1 THEN $2 ELSE title END, \
             year = CASE WHEN $1 THEN $3 ELSE COALESCE(year, $3) END, \
             library_type = CASE WHEN $1 THEN $4 ELSE library_type END, \
             external_imdb = COALESCE($5, external_imdb), \
             external_tvdb_series = COALESCE($6, external_tvdb_series), \
             external_anilist = COALESCE($7, external_anilist), \
             metadata_json = CASE \
                 WHEN metadata_json = '{\"classifierPlaceholder\":true}' \
                      AND ($5 IS NOT NULL OR $6 IS NOT NULL OR $7 IS NOT NULL) \
                 THEN $8 \
                 ELSE COALESCE($8, metadata_json) \
             END, \
             updated_at = CURRENT_TIMESTAMP WHERE id = $9",
        )
        .bind(has_identity_lock)
        .bind(&title)
        .bind(year)
        .bind(media_type.as_str())
        .bind(ids.imdb.as_ref())
        .bind(ids.tvdb_series.as_ref().or(ids.tvdb.as_ref()))
        .bind(ids.anilist.as_ref())
        .bind(meta.and_then(|m| serde_json::to_string(&m.metadata_json).ok()))
        .bind(&id_str)
        .execute(pool)
        .await?;
        return Ok(id);
    }

    let id = Uuid::new_v4();
    sqlx::query::<sqlx::Any>(
        "INSERT INTO series (id, title, year, library_type, external_imdb, external_tvdb_series, external_anilist, metadata_json, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(id.to_string())
    .bind(&identity.title)
    .bind(identity.year)
    .bind(identity.r#type.as_str())
    .bind(merged_ids.imdb.as_ref())
    .bind(merged_ids.tvdb_series.as_ref().or(merged_ids.tvdb.as_ref()))
    .bind(merged_ids.anilist.as_ref())
    .bind(meta.and_then(|m| serde_json::to_string(&m.metadata_json).ok()))
    .execute(pool)
    .await?;
    Ok(id)
}

async fn upsert_unresolved_series_stub(pool: &AnyPool, identity: &MediaIdentity) -> Result<Uuid> {
    let existing = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT id FROM series \
         WHERE title = $1 AND (year = $2 OR year IS NULL OR $3 IS NULL) \
         ORDER BY CASE WHEN year = $2 THEN 0 ELSE 1 END, id LIMIT 1",
    )
    .bind(&identity.title)
    .bind(identity.year)
    .bind(identity.year)
    .fetch_optional(pool)
    .await?;
    let (series_id, created) = if let Some(id) = existing {
        (Uuid::parse_str(&id)?, false)
    } else {
        (
            upsert_series(pool, identity, &ExternalIds::default(), None).await?,
            true,
        )
    };
    if created {
        sqlx::query::<sqlx::Any>(
            "UPDATE series SET metadata_json = '{\"classifierPlaceholder\":true}' \
             WHERE id = $1 AND external_imdb IS NULL AND external_tvdb_series IS NULL \
             AND external_anilist IS NULL",
        )
        .bind(series_id.to_string())
        .execute(pool)
        .await?;
    }

    sqlx::query::<sqlx::Any>(
        "INSERT INTO media_items (id, type, external_ids, title, year, season, episode, \
         metadata_json, runtime_seconds, created_at, updated_at) \
         VALUES ($1, $2, '{}', $3, $4, NULL, NULL, NULL, NULL, \
                 CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) \
         ON CONFLICT(id) DO NOTHING",
    )
    .bind(series_id.to_string())
    .bind(identity.r#type.as_str())
    .bind(&identity.title)
    .bind(identity.year)
    .execute(pool)
    .await?;

    Ok(series_id)
}

async fn load_series_season_state(
    pool: &AnyPool,
    series_id: Uuid,
    season_anilist_seeds: &mut HashMap<i32, SeasonAnilistSeed>,
) -> Result<HashMap<i32, Uuid>> {
    let persisted_seasons = sqlx::query(
        "SELECT s.id, s.season_number, \
         COALESCE(NULLIF(s.external_anilist, ''), (\
             SELECT sei.external_id FROM season_external_ids sei \
             WHERE sei.season_id = s.id AND sei.provider = 'anilist' \
             ORDER BY CASE WHEN sei.confidence IS NULL THEN 1 ELSE 0 END, \
                      sei.confidence DESC, sei.created_at DESC, sei.external_id LIMIT 1\
         ), '') AS external_anilist \
         FROM seasons s WHERE s.series_id = $1 ORDER BY s.season_number",
    )
    .bind(series_id.to_string())
    .fetch_all(pool)
    .await?;

    let mut season_ids = HashMap::new();
    for row in persisted_seasons {
        let season_number = row.try_get::<i64, _>("season_number")? as i32;
        let season_id = Uuid::parse_str(&row.try_get::<String, _>("id")?)?;
        season_ids.insert(season_number, season_id);
        let persisted_anilist_id: String = row.try_get("external_anilist")?;
        let persisted_anilist_id = persisted_anilist_id.trim();
        if !persisted_anilist_id.is_empty() {
            insert_season_anilist_seed(
                season_anilist_seeds,
                season_number,
                SeasonAnilistSeed {
                    anilist_id: persisted_anilist_id.to_string(),
                    confidence: 0.5,
                    causal_paths: BTreeSet::new(),
                },
            );
        }
    }
    Ok(season_ids)
}

async fn upsert_season(pool: &AnyPool, series_id: Uuid, season_number: i32) -> Result<Uuid> {
    let proposed_id = Uuid::new_v4();
    let series_id = series_id.to_string();
    sqlx::query::<sqlx::Any>(
        "INSERT INTO seasons (id, series_id, season_number, created_at, updated_at) \
         VALUES ($1, $2, $3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) \
         ON CONFLICT(series_id, season_number) DO UPDATE SET updated_at = CURRENT_TIMESTAMP",
    )
    .bind(proposed_id.to_string())
    .bind(&series_id)
    .bind(season_number)
    .execute(pool)
    .await?;

    let id = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT id FROM seasons WHERE series_id = $1 AND season_number = $2 LIMIT 1",
    )
    .bind(&series_id)
    .bind(season_number)
    .fetch_one(pool)
    .await?;
    Ok(Uuid::parse_str(&id)?)
}

async fn upsert_episode(
    pool: &AnyPool,
    series_id: Uuid,
    season_id: Uuid,
    season_number: i32,
    episode_number: i32,
    absolute_episode_number: Option<i32>,
) -> Result<Uuid> {
    let proposed_id = Uuid::new_v4();
    let series_id = series_id.to_string();
    sqlx::query::<sqlx::Any>(
        "INSERT INTO episodes \
         (id, series_id, season_id, season_number, episode_number, absolute_episode_number, \
          has_file, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, FALSE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) \
         ON CONFLICT(series_id, season_number, episode_number) DO UPDATE SET \
         absolute_episode_number = COALESCE( \
             excluded.absolute_episode_number, episodes.absolute_episode_number \
         ), \
         updated_at = CURRENT_TIMESTAMP",
    )
    .bind(proposed_id.to_string())
    .bind(&series_id)
    .bind(season_id.to_string())
    .bind(season_number)
    .bind(episode_number)
    .bind(absolute_episode_number)
    .execute(pool)
    .await?;

    let id = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT id FROM episodes \
         WHERE series_id = $1 AND season_number = $2 AND episode_number = $3 LIMIT 1",
    )
    .bind(&series_id)
    .bind(season_number)
    .bind(episode_number)
    .fetch_one(pool)
    .await?;
    Ok(Uuid::parse_str(&id)?)
}

async fn upsert_legacy_media_item(
    pool: &AnyPool,
    id: Uuid,
    identity: &MediaIdentity,
    merged_ids: &ExternalIds,
    meta: Option<&MetadataResult>,
    preserve_existing_identity: bool,
) -> Result<()> {
    let existing = sqlx::query("SELECT external_ids FROM media_items WHERE id = $1 LIMIT 1")
        .bind(id.to_string())
        .fetch_optional(pool)
        .await?;

    let persisted_ids = existing
        .as_ref()
        .and_then(|row| row.try_get::<String, _>("external_ids").ok())
        .and_then(|raw| serde_json::from_str::<ExternalIds>(&raw).ok())
        .unwrap_or_default();
    let effective_ids = merge_external_ids(&persisted_ids, Some(merged_ids.clone()));
    let external_ids_json = serde_json::to_string(&effective_ids)?;
    if existing.is_some() {
        sqlx::query::<sqlx::Any>(
            "UPDATE media_items SET \
             type = CASE WHEN $1 THEN type ELSE $2 END, \
             title = CASE WHEN $1 THEN title ELSE $3 END, \
             year = CASE WHEN $1 THEN COALESCE(year, $4) ELSE $4 END, \
             external_ids = $5, metadata_json = COALESCE($6, metadata_json), \
             runtime_seconds = COALESCE($7, runtime_seconds), \
             updated_at = CURRENT_TIMESTAMP WHERE id = $8",
        )
        .bind(preserve_existing_identity)
        .bind(identity.r#type.as_str())
        .bind(&identity.title)
        .bind(identity.year)
        .bind(external_ids_json)
        .bind(meta.and_then(|m| serde_json::to_string(&m.metadata_json).ok()))
        .bind(meta.and_then(|m| m.runtime_seconds))
        .bind(id.to_string())
        .execute(pool)
        .await?;
    } else {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO media_items (id, type, external_ids, title, year, season, episode, metadata_json, runtime_seconds, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, NULL, NULL, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .bind(id.to_string())
        .bind(identity.r#type.as_str())
        .bind(external_ids_json)
        .bind(&identity.title)
        .bind(identity.year)
        .bind(meta.and_then(|m| serde_json::to_string(&m.metadata_json).ok()))
        .bind(meta.and_then(|m| m.runtime_seconds))
        .execute(pool)
        .await?;
    }

    ExtensionStore::new(pool)
        .upsert_external_media_ownership_if_missing(
            id,
            identity.r#type,
            &identity.title,
            identity.year,
            Some(merged_ids),
        )
        .await?;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct MediaFileUpsert {
    id: Uuid,
    duration_seconds: Option<i32>,
}

async fn retain_files_without_completed_anime_repair(
    pool: &AnyPool,
    candidate: &mut AggregatedCandidate,
    hash_dedupe: bool,
    authoritative_file_paths: &HashSet<String>,
) -> Result<()> {
    let mut retained = Vec::with_capacity(candidate.files.len());
    for file in std::mem::take(&mut candidate.files) {
        if authoritative_file_paths.contains(&file.descriptor.path) {
            retained.push(file);
            continue;
        }
        let completed_media_file_id = if hash_dedupe {
            if let Some(hash) = file
                .descriptor
                .hash
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                sqlx::query_scalar::<sqlx::Any, String>(
                    "SELECT mf.id FROM media_files mf \
                     JOIN library_anime_repairs lar ON lar.media_file_id = mf.id \
                     WHERE mf.hash = $1 AND lar.repair_version = $2 \
                       AND lar.status = 'completed' ORDER BY mf.id LIMIT 1",
                )
                .bind(hash)
                .bind(ANIME_LIBRARY_REPAIR_VERSION)
                .fetch_optional(pool)
                .await?
            } else {
                None
            }
        } else {
            None
        };
        let completed_media_file_id = match completed_media_file_id {
            Some(media_file_id) => Some(media_file_id),
            None => {
                sqlx::query_scalar::<sqlx::Any, String>(
                    "SELECT mf.id FROM media_files mf \
                     JOIN library_anime_repairs lar ON lar.media_file_id = mf.id \
                     WHERE mf.path = $1 AND lar.repair_version = $2 \
                       AND lar.status = 'completed' LIMIT 1",
                )
                .bind(&file.descriptor.path)
                .bind(ANIME_LIBRARY_REPAIR_VERSION)
                .fetch_optional(pool)
                .await?
            }
        };

        let Some(media_file_id) = completed_media_file_id else {
            retained.push(file);
            continue;
        };

        // Identity for this repair version is final, but the physical file can
        // be replaced in place by a higher-quality encode. Refresh its probe,
        // technical columns, tracks, and subtitles while leaving ownership and
        // episode/movie links untouched.
        refresh_existing_media_file_technical_state(
            pool,
            &media_file_id,
            file.source_config_id,
            &file.descriptor,
            Some(&file.extension_metadata),
        )
        .await?;
        tracing::trace!(
            media_file_id = %media_file_id,
            path = %file.descriptor.path,
            repair_version = ANIME_LIBRARY_REPAIR_VERSION,
            "retained completed anime repair identity during library rescan"
        );
    }
    candidate.files = retained;
    Ok(())
}

async fn probe_media_file_for_ingest(
    file: &FileDescriptor,
) -> std::result::Result<ffprobe::MediaMetadata, String> {
    match ffprobe::probe(&file.path).await {
        Ok(metadata) => Ok(metadata),
        Err(error) => {
            tracing::warn!(path = %file.path, error = %error, "ffprobe failed during ingest");
            Err(error.to_string())
        }
    }
}

async fn refresh_existing_media_file_technical_state(
    pool: &AnyPool,
    media_file_id: &str,
    source_config_id: Option<Uuid>,
    file: &FileDescriptor,
    extension_metadata: Option<&HashMap<String, serde_json::Value>>,
) -> Result<Option<i32>> {
    let probe_result = probe_media_file_for_ingest(file).await;
    refresh_existing_media_file_technical_state_with_probe(
        pool,
        media_file_id,
        source_config_id,
        file,
        extension_metadata,
        &probe_result,
    )
    .await
}

async fn refresh_existing_media_file_technical_state_with_probe(
    pool: &AnyPool,
    media_file_id: &str,
    source_config_id: Option<Uuid>,
    file: &FileDescriptor,
    extension_metadata: Option<&HashMap<String, serde_json::Value>>,
    probe_result: &std::result::Result<ffprobe::MediaMetadata, String>,
) -> Result<Option<i32>> {
    let id = Uuid::parse_str(media_file_id)?;
    let existing_source = sqlx::query_scalar::<sqlx::Any, Option<String>>(
        "SELECT source_config_id FROM media_files WHERE id = $1",
    )
    .bind(media_file_id)
    .fetch_one(pool)
    .await?;
    let desired_source =
        existing_source.or_else(|| source_config_id.map(|value| value.to_string()));
    let metadata = probe_result.as_ref().ok();
    let updated = sqlx::query::<sqlx::Any>(
        "UPDATE media_files SET size_bytes = $1, container = $2, video_codec = $3, \
         audio_codec = $4, width = COALESCE($5, width), height = COALESCE($6, height), \
         bitrate_bps = COALESCE($7, bitrate_bps), hash = COALESCE($8, hash), \
         extension_metadata = COALESCE($9, extension_metadata), \
         updated_at = CURRENT_TIMESTAMP, scan_state = 'ok', \
         source_config_id = COALESCE(source_config_id, $10) WHERE id = $11",
    )
    .bind(file.size_bytes)
    .bind(
        metadata
            .and_then(|value| value.container.as_ref())
            .or(file.container.as_ref()),
    )
    .bind(
        metadata
            .and_then(|value| value.video_codec.as_ref())
            .or(file.video_codec.as_ref()),
    )
    .bind(
        metadata
            .and_then(|value| value.audio_codec.as_ref())
            .or(file.audio_codec.as_ref()),
    )
    .bind(metadata.and_then(|value| value.width))
    .bind(metadata.and_then(|value| value.height))
    .bind(metadata.and_then(|value| value.bitrate_bps))
    .bind(file.hash.as_ref())
    .bind(extension_metadata.and_then(|value| serde_json::to_string(value).ok()))
    .bind(desired_source)
    .bind(media_file_id)
    .execute(pool)
    .await?;
    anyhow::ensure!(
        updated.rows_affected() == 1,
        "cannot refresh missing media file {media_file_id}"
    );
    match probe_result {
        Ok(metadata) => {
            playback_probe::upsert_media_file_probe_success(
                pool,
                media_file_id,
                &file.path,
                metadata,
            )
            .await?;
            sync_media_tracks(pool, id, metadata).await?;
        }
        Err(error) => {
            playback_probe::upsert_media_file_probe_failure(pool, media_file_id, &file.path, error)
                .await?;
        }
    }
    sync_external_subtitles(pool, id, &file.path).await?;
    Ok(metadata.and_then(|value| value.duration_seconds))
}

async fn upsert_media_file(
    pool: &AnyPool,
    legacy_item_id: Uuid,
    source_config_id: Option<Uuid>,
    file: &FileDescriptor,
    extension_metadata: Option<&HashMap<String, serde_json::Value>>,
    hash_dedupe: bool,
) -> Result<MediaFileUpsert> {
    let probe_result = probe_media_file_for_ingest(file).await;
    let metadata = probe_result.as_ref().ok();

    let mut existing = None;
    if hash_dedupe {
        if let Some(hash) = &file.hash {
            existing = sqlx::query::<sqlx::Any>(
                "SELECT id, source_config_id FROM media_files WHERE hash = $1 LIMIT 1",
            )
            .bind(hash)
            .fetch_optional(pool)
            .await?;
        }
    }
    if existing.is_none() {
        existing = sqlx::query::<sqlx::Any>(
            "SELECT id, source_config_id FROM media_files WHERE path = $1 LIMIT 1",
        )
        .bind(&file.path)
        .fetch_optional(pool)
        .await?;
    }

    if let Some(row) = existing {
        let id_str: String = row.get(0);
        let id = Uuid::parse_str(&id_str)?;
        let duration_seconds = refresh_existing_media_file_technical_state_with_probe(
            pool,
            &id_str,
            source_config_id,
            file,
            extension_metadata,
            &probe_result,
        )
        .await?;
        return Ok(MediaFileUpsert {
            id,
            duration_seconds,
        });
    }

    let id = Uuid::new_v4();
    sqlx::query::<sqlx::Any>("INSERT INTO media_files (id, media_item_id, source_config_id, path, size_bytes, container, video_codec, audio_codec, width, height, bitrate_bps, hash, extension_metadata, scan_state, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 'ok', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)")
        .bind(id.to_string())
        .bind(legacy_item_id.to_string())
        .bind(source_config_id.map(|u| u.to_string()))
        .bind(&file.path)
        .bind(file.size_bytes)
        .bind(metadata.and_then(|m| m.container.as_ref()).or(file.container.as_ref()))
        .bind(metadata.and_then(|m| m.video_codec.as_ref()).or(file.video_codec.as_ref()))
        .bind(metadata.and_then(|m| m.audio_codec.as_ref()).or(file.audio_codec.as_ref()))
        .bind(metadata.and_then(|m| m.width))
        .bind(metadata.and_then(|m| m.height))
        .bind(metadata.and_then(|m| m.bitrate_bps))
        .bind(file.hash.as_ref())
        .bind(
            extension_metadata
                .and_then(|m| serde_json::to_string(m).ok()),
        )
        .execute(pool)
        .await?;
    if let Ok(metadata) = &probe_result {
        playback_probe::upsert_media_file_probe_success(
            pool,
            &id.to_string(),
            &file.path,
            metadata,
        )
        .await?;
        sync_media_tracks(pool, id, metadata).await?;
    } else if let Err(error) = &probe_result {
        playback_probe::upsert_media_file_probe_failure(pool, &id.to_string(), &file.path, error)
            .await?;
    }
    sync_external_subtitles(pool, id, &file.path).await?;

    Ok(MediaFileUpsert {
        id,
        duration_seconds: metadata.and_then(|m| m.duration_seconds),
    })
}

#[derive(Debug, Clone)]
struct SidecarSubtitle {
    path: String,
    language: Option<String>,
    title: Option<String>,
    format: Option<String>,
    is_default: bool,
    is_forced: bool,
    is_hearing_impaired: bool,
}

async fn sync_media_tracks(
    pool: &AnyPool,
    media_file_id: Uuid,
    metadata: &ffprobe::MediaMetadata,
) -> Result<()> {
    if metadata.streams.is_empty() {
        return Ok(());
    }

    sqlx::query::<sqlx::Any>("DELETE FROM media_tracks WHERE media_file_id = $1")
        .bind(media_file_id.to_string())
        .execute(pool)
        .await?;

    for stream in &metadata.streams {
        let track_type = match stream.codec_type.as_deref() {
            Some("video") => "video",
            Some("audio") => "audio",
            Some("subtitle") => "subtitle",
            _ => continue,
        };
        let language = normalize_language_tag(read_tag(&stream.tags, "language"));
        let title = read_tag(&stream.tags, "title");
        let is_default = stream
            .disposition
            .as_ref()
            .and_then(|d| d.default_flag)
            .unwrap_or(0)
            == 1;
        let is_forced = stream
            .disposition
            .as_ref()
            .and_then(|d| d.forced)
            .unwrap_or(0)
            == 1;

        let id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>("INSERT INTO media_tracks (id, media_file_id, track_type, language, title, codec, channels, is_default, is_forced, stream_index, metadata_json, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)")
            .bind(id.to_string())
            .bind(media_file_id.to_string())
            .bind(track_type)
            .bind(language)
            .bind(title)
            .bind(stream.codec_name.as_ref())
            .bind(stream.channels)
            .bind(is_default)
            .bind(is_forced)
            .bind(stream.index)
            .bind(serde_json::to_string(stream).ok())
            .execute(pool)
            .await?;
    }

    Ok(())
}

async fn sync_external_subtitles(
    pool: &AnyPool,
    media_file_id: Uuid,
    media_path: &str,
) -> Result<()> {
    let subtitles = match discover_sidecar_subtitles(media_path).await {
        Ok(subtitles) => subtitles,
        Err(err) => {
            tracing::warn!(
                path = %media_path,
                error = %err,
                "failed to scan sidecar subtitles"
            );
            return Ok(());
        }
    };

    sqlx::query::<sqlx::Any>("DELETE FROM external_subtitles WHERE media_file_id = $1")
        .bind(media_file_id.to_string())
        .execute(pool)
        .await?;

    for subtitle in subtitles {
        let id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>("INSERT INTO external_subtitles (id, media_file_id, path, language, title, format, is_default, is_forced, is_hearing_impaired, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)")
            .bind(id.to_string())
            .bind(media_file_id.to_string())
            .bind(subtitle.path)
            .bind(subtitle.language)
            .bind(subtitle.title)
            .bind(subtitle.format)
            .bind(subtitle.is_default)
            .bind(subtitle.is_forced)
            .bind(subtitle.is_hearing_impaired)
            .execute(pool)
            .await?;
    }

    Ok(())
}

async fn discover_sidecar_subtitles(path: &str) -> Result<Vec<SidecarSubtitle>> {
    let media_path = Path::new(path);
    let parent = match media_path.parent() {
        Some(parent) => parent,
        None => return Ok(Vec::new()),
    };
    let base_stem = match media_path.file_stem().and_then(|s| s.to_str()) {
        Some(stem) => stem,
        None => return Ok(Vec::new()),
    };

    let mut seen = HashSet::new();
    let mut subtitles = Vec::new();
    let mut dirs = Vec::new();
    dirs.push(parent.to_path_buf());
    dirs.extend(collect_subtitle_dirs(parent).await?);
    for dir in dirs {
        scan_sidecar_dir(base_stem, &dir, &mut seen, &mut subtitles).await?;
    }

    Ok(subtitles)
}

fn parse_sidecar_tokens(base_stem: &str, subtitle_stem: &str) -> Option<Vec<String>> {
    let base_tokens = tokenize_stem(base_stem);
    let mut base_values: Vec<String> = base_tokens
        .into_iter()
        .filter(|token| !token.in_brackets)
        .map(|token| token.value)
        .collect();
    base_values = strip_release_tokens(&base_values);
    if base_values.is_empty() {
        return None;
    }

    let subtitle_values: Vec<String> = tokenize_stem(subtitle_stem)
        .into_iter()
        .map(|token| token.value)
        .collect();
    if subtitle_values.is_empty() {
        return None;
    }

    let consumed = match_sidecar_prefix(&base_values, &subtitle_values)?;
    Some(subtitle_values.into_iter().skip(consumed).collect())
}

fn parse_sidecar_attributes(
    tokens: &[String],
) -> (Option<String>, Option<String>, bool, bool, bool) {
    let mut language = None;
    let mut is_default = false;
    let mut is_forced = false;
    let mut is_hearing_impaired = false;
    let mut title_parts = Vec::new();

    let mut idx = 0;
    while idx < tokens.len() {
        let token = &tokens[idx];
        let token_lower = token.to_ascii_lowercase();
        match token_lower.as_str() {
            "default" | "def" => {
                is_default = true;
            }
            "forced" | "force" => {
                is_forced = true;
            }
            "sdh" | "cc" => {
                is_hearing_impaired = true;
                push_title_tag(&mut title_parts, "SDH");
            }
            "hearing_impaired" | "hearing-impaired" | "hearingimpaired" => {
                is_hearing_impaired = true;
                push_title_tag(&mut title_parts, "HI");
            }
            "hi" if language.is_some() => {
                is_hearing_impaired = true;
                push_title_tag(&mut title_parts, "HI");
            }
            "signs" | "sign" | "songs" | "signsandsongs" | "signs-songs" | "signs_songs" => {
                push_title_tag(&mut title_parts, "Signs");
            }
            "commentary" | "comment" | "commentaries" => {
                push_title_tag(&mut title_parts, "Commentary");
            }
            _ => {
                if language.is_none() && normalize_language_token(&token_lower).is_some() {
                    let mut raw_parts = vec![token.clone()];
                    let mut lookahead = idx + 1;
                    if let Some(next) = tokens.get(lookahead) {
                        let next_lower = next.to_ascii_lowercase();
                        if normalize_script_token(&next_lower).is_some()
                            || normalize_region_token(&next_lower).is_some()
                        {
                            raw_parts.push(next.clone());
                            lookahead += 1;
                            if let Some(next2) = tokens.get(lookahead) {
                                let next2_lower = next2.to_ascii_lowercase();
                                if normalize_region_token(&next2_lower).is_some() {
                                    raw_parts.push(next2.clone());
                                    lookahead += 1;
                                }
                            }
                        } else if is_language_region_name(&next_lower) {
                            raw_parts.push(next.clone());
                            lookahead += 1;
                        }
                    }
                    let raw_tag = raw_parts.join("-");
                    language = normalize_language_value(&raw_tag);
                    idx = lookahead;
                    continue;
                }
                if let Some(region) = region_from_name(&token_lower) {
                    if let Some(current) = language.as_ref() {
                        if current.eq_ignore_ascii_case("pt") {
                            language = Some(format!("pt-{}", region));
                            idx += 1;
                            continue;
                        }
                    }
                }
                title_parts.push(token.clone());
            }
        }
        idx += 1;
    }

    let title = if title_parts.is_empty() {
        None
    } else {
        Some(title_parts.join(" "))
    };

    (language, title, is_default, is_forced, is_hearing_impaired)
}

fn normalize_language_tag(value: Option<String>) -> Option<String> {
    value.and_then(|v| normalize_language_value(&v))
}

fn normalize_language_token(token: &str) -> Option<String> {
    let token = token.trim();
    if token.len() == 2 && token.chars().all(|c| c.is_ascii_alphabetic()) {
        return Some(token.to_ascii_lowercase());
    }
    if token.len() == 3 && token.chars().all(|c| c.is_ascii_alphabetic()) {
        let lower = token.to_ascii_lowercase();
        if let Some(mapped) = map_three_letter_lang(&lower) {
            return Some(mapped.to_string());
        }
        return Some(lower);
    }
    if let Some(mapped) = map_language_name(token) {
        return Some(mapped.to_string());
    }
    None
}

fn normalize_language_value(raw: &str) -> Option<String> {
    let value = raw.trim();
    if value.is_empty() {
        return None;
    }
    let normalized = value.replace('_', "-");
    let mut parts = normalized.split('-').filter(|p| !p.is_empty());
    let first = parts.next()?;
    let language = normalize_language_token(&first.to_ascii_lowercase())?;

    let mut tag_parts = vec![language];
    for part in parts {
        let lower = part.to_ascii_lowercase();
        if let Some(script) = normalize_script_token(&lower) {
            tag_parts.push(script);
        } else if let Some(region) = normalize_region_token(&lower) {
            tag_parts.push(region);
        } else {
            tag_parts.push(lower);
        }
    }

    Some(tag_parts.join("-"))
}

fn normalize_script_token(token: &str) -> Option<String> {
    if token.len() == 4 && token.chars().all(|c| c.is_ascii_alphabetic()) {
        let mut chars = token.chars();
        let first = chars.next().unwrap().to_ascii_uppercase();
        let rest: String = chars.map(|c| c.to_ascii_lowercase()).collect();
        return Some(format!("{first}{rest}"));
    }
    None
}

fn normalize_region_token(token: &str) -> Option<String> {
    if token.len() == 2 && token.chars().all(|c| c.is_ascii_alphabetic()) {
        return Some(token.to_ascii_uppercase());
    }
    if token.len() == 3 && token.chars().all(|c| c.is_ascii_digit()) {
        return Some(token.to_string());
    }
    None
}

fn read_tag(tags: &Option<HashMap<String, String>>, key: &str) -> Option<String> {
    tags.as_ref()
        .and_then(|tags| {
            tags.iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(key))
                .map(|(_, v)| v.clone())
        })
        .and_then(|v| if v.trim().is_empty() { None } else { Some(v) })
}

fn is_sidecar_subtitle_ext(ext: &str) -> bool {
    matches!(
        ext,
        "srt" | "ass" | "ssa" | "vtt" | "sub" | "idx" | "sup" | "smi"
    )
}

fn is_subtitle_dir_name(name: &str) -> bool {
    matches!(name, "subs" | "subtitles" | "subtitle")
}

fn is_language_region_name(token: &str) -> bool {
    matches!(
        token,
        "us" | "usa" | "uk" | "gb" | "brazil" | "br" | "brazilian"
    )
}

fn region_from_name(token: &str) -> Option<&'static str> {
    match token {
        "br" | "brazil" | "brazilian" => Some("BR"),
        "us" | "usa" => Some("US"),
        "uk" | "gb" => Some("GB"),
        _ => None,
    }
}

fn push_title_tag(parts: &mut Vec<String>, tag: &str) {
    if parts.iter().any(|item| item.eq_ignore_ascii_case(tag)) {
        return;
    }
    parts.push(tag.to_string());
}

fn map_three_letter_lang(token: &str) -> Option<&'static str> {
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

fn map_language_name(token: &str) -> Option<&'static str> {
    match token {
        "english" => Some("en"),
        "spanish" => Some("es"),
        "french" => Some("fr"),
        "german" => Some("de"),
        "italian" => Some("it"),
        "portuguese" => Some("pt"),
        "russian" => Some("ru"),
        "japanese" => Some("ja"),
        "korean" => Some("ko"),
        "chinese" => Some("zh"),
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
        _ => None,
    }
}

#[derive(Debug, Clone)]
struct StemToken {
    value: String,
    in_brackets: bool,
}

fn tokenize_stem(stem: &str) -> Vec<StemToken> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut bracket_depth = 0usize;

    for ch in stem.chars() {
        let is_separator = matches!(ch, '.' | '_' | '-' | ' ' | '\t');
        let is_open = matches!(ch, '[' | '(' | '{');
        let is_close = matches!(ch, ']' | ')' | '}');
        if is_separator || is_open || is_close {
            if !current.is_empty() {
                tokens.push(StemToken {
                    value: current.clone(),
                    in_brackets: bracket_depth > 0,
                });
                current.clear();
            }
            if is_open {
                bracket_depth += 1;
            }
            if is_close && bracket_depth > 0 {
                bracket_depth -= 1;
            }
            continue;
        }
        current.push(ch);
    }

    if !current.is_empty() {
        tokens.push(StemToken {
            value: current,
            in_brackets: bracket_depth > 0,
        });
    }

    tokens
}

fn strip_release_tokens(tokens: &[String]) -> Vec<String> {
    let mut cleaned = Vec::new();
    let mut idx = 0;
    while idx < tokens.len() {
        let lower = tokens[idx].to_ascii_lowercase();
        let next_lower = tokens.get(idx + 1).map(|v| v.to_ascii_lowercase());

        if (lower == "web" && matches!(next_lower.as_deref(), Some("dl") | Some("rip")))
            || (lower == "blu" && matches!(next_lower.as_deref(), Some("ray")))
        {
            idx += 2;
            continue;
        }

        if is_release_token(&lower) {
            idx += 1;
            continue;
        }

        cleaned.push(tokens[idx].clone());
        idx += 1;
    }
    cleaned
}

fn is_release_token(token: &str) -> bool {
    if token.is_empty() {
        return true;
    }
    if looks_like_resolution(token) {
        return true;
    }
    if looks_like_codec(token) {
        return true;
    }
    if looks_like_audio_tag(token) {
        return true;
    }

    matches!(
        token,
        "bluray"
            | "blu-ray"
            | "brrip"
            | "bdrip"
            | "hdrip"
            | "webrip"
            | "webdl"
            | "dvdrip"
            | "dvd"
            | "cam"
            | "ts"
            | "tc"
            | "proper"
            | "repack"
            | "remux"
            | "hdr"
            | "hdr10"
            | "hdr10plus"
            | "dv"
            | "dolbyvision"
            | "atmos"
            | "extended"
            | "uncut"
            | "yts"
            | "rarbg"
            | "ettv"
    )
}

fn looks_like_resolution(token: &str) -> bool {
    if token == "4k" {
        return true;
    }
    if let Some(stripped) = token.strip_suffix('p') {
        return stripped.chars().all(|c| c.is_ascii_digit());
    }
    if let Some((w, h)) = token.split_once('x') {
        return w.chars().all(|c| c.is_ascii_digit()) && h.chars().all(|c| c.is_ascii_digit());
    }
    false
}

fn looks_like_codec(token: &str) -> bool {
    matches!(
        token,
        "x264" | "x265" | "h264" | "h265" | "hevc" | "av1" | "vp9"
    )
}

fn looks_like_audio_tag(token: &str) -> bool {
    matches!(
        token,
        "aac" | "ac3" | "eac3" | "truehd" | "dts" | "dtsx" | "flac" | "opus"
    )
}

fn match_sidecar_prefix(base_tokens: &[String], subtitle_tokens: &[String]) -> Option<usize> {
    let mut base_idx = 0usize;
    let mut sub_idx = 0usize;
    while base_idx < base_tokens.len() {
        if sub_idx >= subtitle_tokens.len() {
            return None;
        }
        let base_norm = normalize_match_token(&base_tokens[base_idx]);
        let sub_norm = normalize_match_token(&subtitle_tokens[sub_idx]);

        if base_norm == sub_norm {
            base_idx += 1;
            sub_idx += 1;
            continue;
        }

        if let Some(base_parts) = split_episode_token(&base_norm) {
            if tokens_match_sequence(&base_parts, subtitle_tokens, sub_idx) {
                base_idx += 1;
                sub_idx += base_parts.len();
                continue;
            }
        }

        if let Some(sub_parts) = split_episode_token(&sub_norm) {
            if tokens_match_sequence(&sub_parts, base_tokens, base_idx) {
                base_idx += sub_parts.len();
                sub_idx += 1;
                continue;
            }
        }

        return None;
    }
    Some(sub_idx)
}

fn tokens_match_sequence(parts: &[String], tokens: &[String], start: usize) -> bool {
    if start + parts.len() > tokens.len() {
        return false;
    }
    for (offset, part) in parts.iter().enumerate() {
        let token_norm = normalize_match_token(&tokens[start + offset]);
        if token_norm != *part {
            return false;
        }
    }
    true
}

fn normalize_match_token(token: &str) -> String {
    let lower = token.to_ascii_lowercase();
    normalize_simple_episode_token(&lower).unwrap_or(lower)
}

fn normalize_simple_episode_token(token: &str) -> Option<String> {
    if let Some(rest) = token.strip_prefix('s') {
        if rest.chars().all(|c| c.is_ascii_digit()) {
            let value = rest.parse::<i32>().ok()?;
            return Some(format!("s{:02}", value));
        }
    }
    if let Some(rest) = token.strip_prefix('e') {
        if rest.chars().all(|c| c.is_ascii_digit()) {
            let value = rest.parse::<i32>().ok()?;
            return Some(format!("e{}", format_episode_number(value)));
        }
    }
    if let Some(rest) = token.strip_prefix("ep") {
        if rest.chars().all(|c| c.is_ascii_digit()) {
            let value = rest.parse::<i32>().ok()?;
            return Some(format!("e{}", format_episode_number(value)));
        }
    }
    None
}

fn split_episode_token(token: &str) -> Option<Vec<String>> {
    let token = token.to_ascii_lowercase();
    if let Some(rest) = token.strip_prefix('s') {
        let e_pos = rest.find('e')?;
        let season_str = &rest[..e_pos];
        let episode_str = &rest[e_pos + 1..];
        if season_str.is_empty() || episode_str.is_empty() {
            return None;
        }
        if !season_str.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        let season = season_str.parse::<i32>().ok()?;
        let mut parts = Vec::new();
        parts.push(format!("s{:02}", season));
        for ep in parse_episode_sequence(episode_str)? {
            parts.push(format!("e{}", format_episode_number(ep)));
        }
        return Some(parts);
    }

    if let Some(x_pos) = token.find('x') {
        let season_str = &token[..x_pos];
        let episode_str = &token[x_pos + 1..];
        if season_str.is_empty() || episode_str.is_empty() {
            return None;
        }
        if !season_str.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        let season = season_str.parse::<i32>().ok()?;
        let mut parts = Vec::new();
        parts.push(format!("s{:02}", season));
        for ep in parse_episode_sequence(episode_str)? {
            parts.push(format!("e{}", format_episode_number(ep)));
        }
        return Some(parts);
    }

    if let Some(rest) = token.strip_prefix('e') {
        let mut parts = Vec::new();
        for ep in parse_episode_sequence(rest)? {
            parts.push(format!("e{}", format_episode_number(ep)));
        }
        return Some(parts);
    }

    None
}

fn parse_episode_sequence(raw: &str) -> Option<Vec<i32>> {
    let normalized = raw.replace('e', "-");
    let mut episodes = Vec::new();
    for part in normalized.split(|c| c == '-' || c == '+') {
        if part.is_empty() {
            continue;
        }
        if !part.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        episodes.push(part.parse::<i32>().ok()?);
    }
    if episodes.is_empty() {
        None
    } else {
        Some(episodes)
    }
}

fn format_episode_number(num: i32) -> String {
    if num >= 100 {
        format!("{:03}", num)
    } else {
        format!("{:02}", num)
    }
}

async fn collect_subtitle_dirs(parent: &Path) -> Result<Vec<PathBuf>> {
    let mut dirs = Vec::new();
    let mut entries = tokio::fs::read_dir(parent).await?;
    while let Some(entry) = entries.next_entry().await? {
        let file_type = entry.file_type().await?;
        if !file_type.is_dir() {
            continue;
        }
        let name = match entry.file_name().to_str() {
            Some(name) => name.to_ascii_lowercase(),
            None => continue,
        };
        if is_subtitle_dir_name(&name) {
            dirs.push(entry.path());
        }
    }
    Ok(dirs)
}

async fn scan_sidecar_dir(
    base_stem: &str,
    dir: &Path,
    seen: &mut HashSet<String>,
    subtitles: &mut Vec<SidecarSubtitle>,
) -> Result<()> {
    let mut entries = tokio::fs::read_dir(dir).await?;
    let mut files = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let file_type = entry.file_type().await?;
        if !file_type.is_file() {
            continue;
        }
        let entry_path = entry.path();
        let ext = match entry_path.extension().and_then(|s| s.to_str()) {
            Some(ext) => ext.to_ascii_lowercase(),
            None => continue,
        };
        if !is_sidecar_subtitle_ext(&ext) {
            continue;
        }
        let stem = match entry_path.file_stem().and_then(|s| s.to_str()) {
            Some(stem) => stem.to_string(),
            None => continue,
        };
        files.push((entry_path, stem, ext));
    }

    let mut idx_stems = HashSet::new();
    for (_, stem, ext) in &files {
        if ext == "idx" {
            idx_stems.insert(stem.to_ascii_lowercase());
        }
    }

    for (entry_path, stem, ext) in files {
        if ext == "sub" && idx_stems.contains(&stem.to_ascii_lowercase()) {
            continue;
        }
        let tokens = match parse_sidecar_tokens(base_stem, &stem) {
            Some(tokens) => tokens,
            None => continue,
        };
        let (language, title, is_default, is_forced, is_hearing_impaired) =
            parse_sidecar_attributes(&tokens);
        let entry_path_string = entry_path.to_string_lossy().to_string();
        if !seen.insert(entry_path_string.clone()) {
            continue;
        }
        subtitles.push(SidecarSubtitle {
            path: entry_path_string,
            language,
            title,
            format: Some(ext),
            is_default,
            is_forced,
            is_hearing_impaired,
        });
    }

    Ok(())
}

pub(crate) fn normalize_override_key(raw: &str) -> Option<String> {
    let tokens = tokenize_stem(raw);
    let values: Vec<String> = tokens
        .into_iter()
        .filter(|token| !token.in_brackets)
        .map(|token| token.value)
        .collect();
    let cleaned = strip_release_tokens(&values);
    let normalized = cleaned.join(" ").trim().to_ascii_lowercase();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

pub(crate) fn derive_override_key(library_type: &str, media_path: &str) -> Option<String> {
    let path = Path::new(media_path);
    let raw = match library_type {
        "movie" => path
            .file_stem()
            .and_then(|s| s.to_str())
            .map(|s| s.to_string()),
        "series" | "anime" => select_series_root_name(path),
        _ => path
            .file_stem()
            .and_then(|s| s.to_str())
            .map(|s| s.to_string()),
    }?;
    normalize_override_key(&raw)
}

fn select_series_root_name(path: &Path) -> Option<String> {
    let mut current = path.parent()?;
    let mut name = current.file_name()?.to_str()?.to_string();
    if is_season_folder_name(&name) {
        current = current.parent()?;
        name = current.file_name()?.to_str()?.to_string();
    }
    Some(name)
}

fn is_season_folder_name(name: &str) -> bool {
    let cleaned: String = name
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase();
    if cleaned.is_empty() {
        return false;
    }
    if cleaned == "specials" || cleaned == "special" {
        return true;
    }
    if let Some(rest) = cleaned.strip_prefix("season") {
        return !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit());
    }
    if let Some(rest) = cleaned.strip_prefix('s') {
        return !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit());
    }
    false
}

async fn link_movie_file(pool: &AnyPool, movie_id: Uuid, media_file_id: Uuid) -> Result<bool> {
    link_movie_file_inner(pool, movie_id, media_file_id, true).await
}

async fn link_movie_file_authoritative(
    pool: &AnyPool,
    movie_id: Uuid,
    media_file_id: Uuid,
) -> Result<bool> {
    link_movie_file_inner(pool, movie_id, media_file_id, false).await
}

async fn link_movie_file_inner(
    pool: &AnyPool,
    movie_id: Uuid,
    media_file_id: Uuid,
    respect_completed_repair: bool,
) -> Result<bool> {
    let movie_id_str = movie_id.to_string();
    let media_file_id_str = media_file_id.to_string();
    let mut transaction = pool.begin().await?;

    let locked_file = sqlx::query::<sqlx::Any>("UPDATE media_files SET id = id WHERE id = $1")
        .bind(&media_file_id_str)
        .execute(&mut *transaction)
        .await?;
    if locked_file.rows_affected() != 1 {
        anyhow::bail!("cannot link missing media file {media_file_id}");
    }

    let target_exists: Option<i64> =
        sqlx::query_scalar::<sqlx::Any, i64>("SELECT 1 FROM movies WHERE id = $1 LIMIT 1")
            .bind(&movie_id_str)
            .fetch_optional(&mut *transaction)
            .await?;
    if target_exists.is_none() {
        anyhow::bail!("cannot link media file {media_file_id} to missing movie {movie_id}");
    }

    let previous_episode_ids: Vec<String> = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT episode_id FROM episode_files WHERE media_file_id = $1 ORDER BY episode_id",
    )
    .bind(&media_file_id_str)
    .fetch_all(&mut *transaction)
    .await?;
    let previous_movie_ids: Vec<String> = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT movie_id FROM movie_files WHERE media_file_id = $1 ORDER BY movie_id",
    )
    .bind(&media_file_id_str)
    .fetch_all(&mut *transaction)
    .await?;

    // A completed ALM-8 repair is the durable winner for this repair version. A scan can begin
    // from stale in-memory evidence, wait behind the repair's per-file write lock, and otherwise
    // replace the freshly repaired link after the repair commits. Check the ledger only after
    // acquiring that same lock and reject a conflicting relink. Matching rescans remain harmless
    // idempotent updates; a future repair algorithm can intentionally supersede this fence by
    // incrementing ANIME_LIBRARY_REPAIR_VERSION.
    let completed_repair: Option<i64> = sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT 1 FROM library_anime_repairs \
         WHERE media_file_id = $1 AND repair_version = $2 AND status = 'completed' LIMIT 1",
    )
    .bind(&media_file_id_str)
    .bind(ANIME_LIBRARY_REPAIR_VERSION)
    .fetch_optional(&mut *transaction)
    .await?;
    let expected_movie_ids = vec![movie_id_str.clone()];
    let conflicts_with_completed_repair = respect_completed_repair
        && completed_repair.is_some()
        && (!previous_episode_ids.is_empty() || previous_movie_ids != expected_movie_ids);
    if conflicts_with_completed_repair {
        tracing::warn!(
            media_file_id = %media_file_id,
            repair_version = ANIME_LIBRARY_REPAIR_VERSION,
            current_episode_ids = ?previous_episode_ids,
            rejected_movie_id = %movie_id_str,
            "discarding stale movie relink after completed anime library repair"
        );
        transaction.rollback().await?;
        return Ok(false);
    }

    sqlx::query::<sqlx::Any>("UPDATE media_files SET media_item_id = $1 WHERE id = $2")
        .bind(&movie_id_str)
        .bind(&media_file_id_str)
        .execute(&mut *transaction)
        .await?;

    if !previous_episode_ids.is_empty() {
        tracing::warn!(
            media_file_id = %media_file_id,
            episode_count = previous_episode_ids.len(),
            "removing episode links for movie-classified file"
        );
        sqlx::query::<sqlx::Any>("DELETE FROM episode_files WHERE media_file_id = $1")
            .bind(&media_file_id_str)
            .execute(&mut *transaction)
            .await?;
    }
    sqlx::query::<sqlx::Any>("DELETE FROM movie_files WHERE media_file_id = $1 AND movie_id != $2")
        .bind(&media_file_id_str)
        .bind(&movie_id_str)
        .execute(&mut *transaction)
        .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO movie_files (movie_id, media_file_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
    )
    .bind(&movie_id_str)
    .bind(&media_file_id_str)
    .execute(&mut *transaction)
    .await?;

    for previous_movie_id in previous_movie_ids {
        if previous_movie_id == movie_id_str {
            continue;
        }
        cleanup_orphan_movie_placeholder_in_transaction(&mut transaction, &previous_movie_id)
            .await?;
    }

    for previous_episode_id in previous_episode_ids {
        refresh_episode_has_file_in_transaction(&mut transaction, &previous_episode_id).await?;
    }

    transaction.commit().await?;
    Ok(true)
}

async fn link_episode_file(pool: &AnyPool, episode_id: Uuid, media_file_id: Uuid) -> Result<()> {
    replace_episode_file_links(pool, media_file_id, &[episode_id]).await
}

async fn link_episode_file_with_classification(
    pool: &AnyPool,
    episode_id: Uuid,
    media_file_id: Uuid,
    outcome: &ClassificationOutcome,
) -> Result<()> {
    replace_episode_file_links_inner(pool, media_file_id, &[episode_id], Some(outcome), true).await
}

async fn replace_episode_file_links(
    pool: &AnyPool,
    media_file_id: Uuid,
    episode_ids: &[Uuid],
) -> Result<()> {
    replace_episode_file_links_inner(pool, media_file_id, episode_ids, None, true).await
}

async fn replace_episode_file_links_authoritative(
    pool: &AnyPool,
    media_file_id: Uuid,
    episode_ids: &[Uuid],
) -> Result<()> {
    replace_episode_file_links_inner(pool, media_file_id, episode_ids, None, false).await
}

async fn replace_episode_file_links_inner(
    pool: &AnyPool,
    media_file_id: Uuid,
    episode_ids: &[Uuid],
    classification: Option<&ClassificationOutcome>,
    respect_completed_repair: bool,
) -> Result<()> {
    let mut target_episode_ids: Vec<String> = episode_ids.iter().map(Uuid::to_string).collect();
    target_episode_ids.sort_unstable();
    target_episode_ids.dedup();
    if target_episode_ids.is_empty() {
        anyhow::bail!("cannot link media file {media_file_id} without target episodes");
    }

    let media_file_id_str = media_file_id.to_string();
    let mut transaction = pool.begin().await?;

    // A no-op UPDATE provides a portable per-file write lock. PostgreSQL locks
    // the selected row and SQLite serializes the write transaction, preventing
    // concurrent classifiers from interleaving stale-link cleanup and insertion.
    let locked_file = sqlx::query::<sqlx::Any>("UPDATE media_files SET id = id WHERE id = $1")
        .bind(&media_file_id_str)
        .execute(&mut *transaction)
        .await?;
    if locked_file.rows_affected() != 1 {
        anyhow::bail!("cannot link missing media file {media_file_id}");
    }

    let previous_media_item_id: String =
        sqlx::query_scalar("SELECT media_item_id FROM media_files WHERE id = $1")
            .bind(&media_file_id_str)
            .fetch_one(&mut *transaction)
            .await?;
    let mut target_series_id: Option<String> = None;
    for target_episode_id in &target_episode_ids {
        let episode_series_id: Option<String> = sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT series_id FROM episodes WHERE id = $1 LIMIT 1",
        )
        .bind(target_episode_id)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(episode_series_id) = episode_series_id else {
            anyhow::bail!(
                "cannot link media file {media_file_id} to missing episode {target_episode_id}"
            );
        };
        if let Some(expected_series_id) = target_series_id.as_ref() {
            if expected_series_id != &episode_series_id {
                anyhow::bail!(
                    "cannot link media file {media_file_id} to episodes from multiple series"
                );
            }
        } else {
            target_series_id = Some(episode_series_id);
        }
    }
    let target_series_id = target_series_id.ok_or_else(|| {
        anyhow::anyhow!("cannot link media file {media_file_id} without a target series")
    })?;

    let previous_episode_ids: Vec<String> = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT episode_id FROM episode_files WHERE media_file_id = $1 ORDER BY episode_id",
    )
    .bind(&media_file_id_str)
    .fetch_all(&mut *transaction)
    .await?;
    let previous_movie_ids: Vec<String> = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT movie_id FROM movie_files WHERE media_file_id = $1 ORDER BY movie_id",
    )
    .bind(&media_file_id_str)
    .fetch_all(&mut *transaction)
    .await?;

    // A completed ALM-8 repair is the durable winner for this repair version. A scan can begin
    // from stale in-memory evidence, wait behind the repair's per-file write lock, and otherwise
    // replace the freshly repaired link after the repair commits. Check the ledger only after
    // acquiring that same lock and reject a conflicting relink. Matching rescans remain harmless
    // idempotent updates; a future repair algorithm can intentionally supersede this fence by
    // incrementing ANIME_LIBRARY_REPAIR_VERSION.
    let completed_repair: Option<i64> = sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT 1 FROM library_anime_repairs \
         WHERE media_file_id = $1 AND repair_version = $2 AND status = 'completed' LIMIT 1",
    )
    .bind(&media_file_id_str)
    .bind(ANIME_LIBRARY_REPAIR_VERSION)
    .fetch_optional(&mut *transaction)
    .await?;
    let conflicts_with_completed_repair = respect_completed_repair
        && completed_repair.is_some()
        && (previous_media_item_id != target_series_id
            || !previous_movie_ids.is_empty()
            || previous_episode_ids != target_episode_ids);
    if conflicts_with_completed_repair {
        tracing::warn!(
            media_file_id = %media_file_id,
            repair_version = ANIME_LIBRARY_REPAIR_VERSION,
            current_episode_ids = ?previous_episode_ids,
            rejected_episode_ids = ?target_episode_ids,
            "discarding stale scan relink after completed anime library repair"
        );
        transaction.rollback().await?;
        return Ok(());
    }

    sqlx::query::<sqlx::Any>("UPDATE media_files SET media_item_id = $1 WHERE id = $2")
        .bind(&target_series_id)
        .bind(&media_file_id_str)
        .execute(&mut *transaction)
        .await?;

    if !previous_movie_ids.is_empty() {
        tracing::warn!(
            media_file_id = %media_file_id,
            movie_count = previous_movie_ids.len(),
            "removing movie links for episode-classified file"
        );
        sqlx::query::<sqlx::Any>("DELETE FROM movie_files WHERE media_file_id = $1")
            .bind(&media_file_id_str)
            .execute(&mut *transaction)
            .await?;
    }

    let target_episode_id_set: HashSet<&str> =
        target_episode_ids.iter().map(String::as_str).collect();
    let stale_episode_count = previous_episode_ids
        .iter()
        .filter(|existing_id| !target_episode_id_set.contains(existing_id.as_str()))
        .count();
    if stale_episode_count > 0 || previous_episode_ids.len() != target_episode_ids.len() {
        tracing::warn!(
            media_file_id = %media_file_id,
            target_episode_count = target_episode_ids.len(),
            stale_episode_count,
            "replacing episode links with canonical complete set"
        );
    }

    sqlx::query::<sqlx::Any>("DELETE FROM episode_files WHERE media_file_id = $1")
        .bind(&media_file_id_str)
        .execute(&mut *transaction)
        .await?;
    for target_episode_id in &target_episode_ids {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO episode_files (episode_id, media_file_id) VALUES ($1, $2)",
        )
        .bind(target_episode_id)
        .bind(&media_file_id_str)
        .execute(&mut *transaction)
        .await?;
    }

    for movie_id in previous_movie_ids {
        let has_file: Option<i64> = sqlx::query_scalar::<sqlx::Any, i64>(
            "SELECT 1 FROM movie_files WHERE movie_id = $1 LIMIT 1",
        )
        .bind(&movie_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if has_file.is_some() {
            continue;
        }

        cleanup_orphan_movie_placeholder_in_transaction(&mut transaction, &movie_id).await?;
    }

    let mut affected_episode_ids: HashSet<String> = previous_episode_ids.into_iter().collect();
    affected_episode_ids.extend(target_episode_ids);
    let mut affected_episode_ids: Vec<String> = affected_episode_ids.into_iter().collect();
    affected_episode_ids.sort_unstable();
    for affected_episode_id in affected_episode_ids {
        refresh_episode_has_file_in_transaction(&mut transaction, &affected_episode_id).await?;
    }

    if let Some(classification) = classification {
        persist_classification_outcome_in_transaction(
            &mut transaction,
            media_file_id,
            classification,
        )
        .await?;
    }

    transaction.commit().await?;
    if let Err(error) =
        cleanup_orphan_series_stub(pool, &previous_media_item_id, &target_series_id).await
    {
        tracing::warn!(
            media_file_id = %media_file_id,
            previous_series_id = %previous_media_item_id,
            current_series_id = %target_series_id,
            error = %error,
            "episode relink committed but classifier placeholder cleanup failed"
        );
    }
    Ok(())
}

async fn refresh_episode_has_file_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    episode_id: &str,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE episodes SET has_file = CASE WHEN EXISTS (\
            SELECT 1 FROM episode_files ef \
            JOIN media_files mf ON mf.id = ef.media_file_id \
            WHERE ef.episode_id = episodes.id AND mf.scan_state = 'ok'\
         ) THEN TRUE ELSE FALSE END, updated_at = CURRENT_TIMESTAMP \
         WHERE id = $1",
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
    // Removing a stale cross-link says nothing about the movie's independent
    // value. Delete only the exact internal placeholder shape; real metadata,
    // external identity, artwork, or managed ownership keeps both DB rows.
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

async fn cleanup_orphan_series_stub(
    pool: &AnyPool,
    previous_media_item_id: &str,
    current_media_item_id: &str,
) -> Result<()> {
    if previous_media_item_id == current_media_item_id {
        return Ok(());
    }

    let removed = sqlx::query::<sqlx::Any>(
        "DELETE FROM series WHERE id = $1 \
         AND external_imdb IS NULL \
         AND external_tvdb_series IS NULL \
         AND external_anilist IS NULL \
         AND metadata_json = '{\"classifierPlaceholder\":true}' \
         AND NOT EXISTS (SELECT 1 FROM series_external_ids WHERE series_id = $1) \
         AND NOT EXISTS (SELECT 1 FROM media_files WHERE media_item_id = $1) \
         AND NOT EXISTS (\
             SELECT 1 FROM episode_files ef \
             JOIN episodes e ON e.id = ef.episode_id \
             WHERE e.series_id = $1\
         )",
    )
    .bind(previous_media_item_id)
    .execute(pool)
    .await?;
    if removed.rows_affected() == 0 {
        return Ok(());
    }

    sqlx::query::<sqlx::Any>(
        "DELETE FROM media_items WHERE id = $1 \
         AND NOT EXISTS (SELECT 1 FROM media_files WHERE media_item_id = $1)",
    )
    .bind(previous_media_item_id)
    .execute(pool)
    .await?;
    tracing::info!(
        series_id = %previous_media_item_id,
        "removed resolved classifier placeholder series"
    );
    Ok(())
}

async fn mark_series_as_anime(pool: &AnyPool, series_id: Uuid) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE series SET library_type = 'anime', updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND library_type != 'anime'",
    )
    .bind(series_id.to_string())
    .execute(pool)
    .await?;
    let _ = sqlx::query::<sqlx::Any>(
        "UPDATE media_items SET type = 'anime', updated_at = CURRENT_TIMESTAMP WHERE id = $1",
    )
    .bind(series_id.to_string())
    .execute(pool)
    .await;
    Ok(())
}

async fn update_movie_runtime_if_missing(
    pool: &AnyPool,
    movie_id: Uuid,
    duration_seconds: i32,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE movies SET runtime_seconds = COALESCE(runtime_seconds, $1), updated_at = CURRENT_TIMESTAMP WHERE id = $2",
    )
    .bind(duration_seconds)
    .bind(movie_id.to_string())
    .execute(pool)
    .await?;

    let _ = sqlx::query::<sqlx::Any>(
        "UPDATE media_items SET runtime_seconds = COALESCE(runtime_seconds, $1), updated_at = CURRENT_TIMESTAMP WHERE id = $2",
    )
    .bind(duration_seconds)
    .bind(movie_id.to_string())
    .execute(pool)
    .await;

    Ok(())
}

async fn update_episode_runtime_if_missing(
    pool: &AnyPool,
    episode_id: Uuid,
    duration_seconds: i32,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE episodes SET runtime_seconds = COALESCE(runtime_seconds, $1), updated_at = CURRENT_TIMESTAMP WHERE id = $2",
    )
    .bind(duration_seconds)
    .bind(episode_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn update_episode_title_if_missing(
    pool: &AnyPool,
    episode_id: Uuid,
    title: &str,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE episodes SET title = COALESCE(title, $1), updated_at = CURRENT_TIMESTAMP WHERE id = $2",
    )
    .bind(title)
    .bind(episode_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn refresh_episode_file_state(pool: &AnyPool) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE episodes SET has_file = CASE WHEN EXISTS (SELECT 1 FROM episode_files ef JOIN media_files mf ON mf.id = ef.media_file_id WHERE ef.episode_id = episodes.id AND mf.scan_state = 'ok') THEN TRUE ELSE FALSE END",
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub(crate) async fn apply_external_ids_to_movie(
    pool: &AnyPool,
    movie_id: Uuid,
    ids: &ExternalIds,
    source: &str,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE movies SET external_imdb = COALESCE($1, external_imdb), external_tmdb = COALESCE($2, external_tmdb), updated_at = CURRENT_TIMESTAMP WHERE id = $3",
    )
    .bind(ids.imdb.as_ref())
    .bind(ids.tmdb.as_ref())
    .bind(movie_id.to_string())
    .execute(pool)
    .await?;

    persist_movie_external_ids(pool, movie_id, ids, source).await?;
    Ok(())
}

pub(crate) async fn apply_external_ids_to_series(
    pool: &AnyPool,
    series_id: Uuid,
    ids: &ExternalIds,
    source: &str,
) -> Result<()> {
    tracing::trace!(
        series_id = %series_id,
        source,
        ids = ?ids,
        "apply external ids to series"
    );
    let tvdb = ids.tvdb_series.as_ref().or(ids.tvdb.as_ref()).cloned();
    sqlx::query::<sqlx::Any>(
        "UPDATE series SET external_imdb = COALESCE($1, external_imdb), external_tvdb_series = COALESCE($2, external_tvdb_series), external_anilist = COALESCE($3, external_anilist), updated_at = CURRENT_TIMESTAMP WHERE id = $4",
    )
    .bind(ids.imdb.as_ref())
    .bind(tvdb.as_ref())
    .bind(ids.anilist.as_ref())
    .bind(series_id.to_string())
    .execute(pool)
    .await?;

    persist_series_external_ids(pool, series_id, ids, source).await?;
    Ok(())
}

pub(crate) async fn apply_external_ids_to_season(
    pool: &AnyPool,
    season_id: Uuid,
    ids: &ExternalIds,
    source: &str,
    confidence: Option<f32>,
) -> Result<()> {
    apply_external_ids_to_season_recording_rows(pool, season_id, ids, source, confidence).await?;
    Ok(())
}

async fn apply_external_ids_to_season_recording_rows(
    pool: &AnyPool,
    season_id: Uuid,
    ids: &ExternalIds,
    source: &str,
    confidence: Option<f32>,
) -> Result<Vec<PersistedExternalIdentityRow>> {
    tracing::trace!(
        season_id = %season_id,
        source,
        ids = ?ids,
        "apply external ids to season"
    );
    if let Some(new_id) = ids.anilist.as_ref() {
        let existing = sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT COALESCE(external_anilist, '') FROM seasons WHERE id = $1 LIMIT 1",
        )
        .bind(season_id.to_string())
        .fetch_optional(pool)
        .await?
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

        let mut should_update = existing.is_none();
        if let Some(existing_id) = existing.as_ref() {
            if existing_id == new_id {
                should_update = false;
            } else if source == "override" {
                should_update = true;
            } else if source == "anime_match" {
                should_update = sqlx::query_scalar::<sqlx::Any, i64>(
                    "SELECT 1 FROM season_external_ids WHERE season_id = $1 \
                     AND provider = 'anilist' AND external_id = $2 \
                     AND source IN ('classifier', 'anilist_chain') LIMIT 1",
                )
                .bind(season_id.to_string())
                .bind(existing_id)
                .fetch_optional(pool)
                .await?
                .is_some();
            } else if let Some(new_confidence) = confidence {
                let existing_confidence = sqlx::query_scalar::<sqlx::Any, f32>(
                    "SELECT MAX(confidence) FROM season_external_ids WHERE season_id = $1 AND provider = 'anilist' AND external_id = $2",
                )
                .bind(season_id.to_string())
                .bind(existing_id)
                .fetch_optional(pool)
                .await?
                .unwrap_or(0.0);
                if new_confidence > existing_confidence {
                    should_update = true;
                }
            }
        }

        if should_update {
            sqlx::query::<sqlx::Any>(
                "UPDATE seasons SET external_anilist = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2",
            )
            .bind(new_id)
            .bind(season_id.to_string())
            .execute(pool)
            .await?;
        }
    }

    persist_season_external_ids(pool, season_id, ids, source, confidence).await
}

async fn persist_movie_external_ids(
    pool: &AnyPool,
    movie_id: Uuid,
    ids: &ExternalIds,
    source: &str,
) -> Result<()> {
    let mut entries: Vec<(&'static str, String)> = Vec::new();
    if let Some(imdb) = ids.imdb.as_ref() {
        entries.push(("imdb", imdb.clone()));
    }
    if let Some(tmdb) = ids.tmdb.as_ref() {
        entries.push(("tmdb", tmdb.clone()));
    }
    if let Some(tvdb) = ids.tvdb_movie.as_ref().or(ids.tvdb.as_ref()) {
        entries.push(("tvdb", tvdb.clone()));
    }

    let stored_count = entries.len();
    for (provider, external_id) in entries {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO movie_external_ids (id, movie_id, provider, external_id, confidence, source) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(movie_id.to_string())
        .bind(provider)
        .bind(external_id)
        .bind(1.0_f32)
        .bind(source)
        .execute(pool)
        .await?;
    }

    tracing::trace!(
        movie_id = %movie_id,
        source,
        stored = stored_count,
        "persisted movie external ids"
    );
    Ok(())
}

async fn persist_series_external_ids(
    pool: &AnyPool,
    series_id: Uuid,
    ids: &ExternalIds,
    source: &str,
) -> Result<Vec<PersistedExternalIdentityRow>> {
    let mut entries: Vec<(&'static str, String)> = Vec::new();
    if let Some(imdb) = ids.imdb.as_ref() {
        entries.push(("imdb", imdb.clone()));
    }
    if let Some(tvdb) = ids.tvdb_series.as_ref().or(ids.tvdb.as_ref()) {
        entries.push(("tvdb", tvdb.clone()));
    }
    if let Some(anilist) = ids.anilist.as_ref() {
        entries.push(("anilist", anilist.clone()));
    }
    if let Some(anidb) = ids.anidb.as_ref() {
        entries.push(("anidb", anidb.clone()));
    }
    if let Some(mal) = ids.mal.as_ref() {
        entries.push(("mal", mal.clone()));
    }
    if let Some(kitsu) = ids.kitsu.as_ref() {
        entries.push(("kitsu", kitsu.clone()));
    }
    if let Some(tmdb) = ids.tmdb.as_ref() {
        entries.push(("tmdb", tmdb.clone()));
    }

    let stored_count = entries.len();
    let mut inserted_rows = Vec::new();
    for (provider, external_id) in entries {
        if source == "anime_match" {
            sqlx::query::<sqlx::Any>(
                "UPDATE series_external_ids SET confidence = 1.0, source = 'anime_match' \
                 WHERE series_id = $1 AND provider = $2 AND external_id = $3 \
                   AND source IN ('classifier', 'anilist_chain')",
            )
            .bind(series_id.to_string())
            .bind(provider)
            .bind(&external_id)
            .execute(pool)
            .await?;
        }
        let result = sqlx::query::<sqlx::Any>(
            "INSERT INTO series_external_ids (id, series_id, provider, external_id, confidence, source) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(series_id.to_string())
        .bind(provider)
        .bind(&external_id)
        .bind(1.0_f32)
        .bind(source)
        .execute(pool)
        .await?;
        if result.rows_affected() == 1 {
            inserted_rows.push(PersistedExternalIdentityRow {
                provider: provider.to_string(),
                external_id,
                source: source.to_string(),
            });
        }
    }

    tracing::trace!(
        series_id = %series_id,
        source,
        stored = stored_count,
        "persisted series external ids"
    );
    Ok(inserted_rows)
}

async fn persist_season_external_ids(
    pool: &AnyPool,
    season_id: Uuid,
    ids: &ExternalIds,
    source: &str,
    confidence: Option<f32>,
) -> Result<Vec<PersistedExternalIdentityRow>> {
    let mut entries: Vec<(&'static str, String)> = Vec::new();
    if let Some(anilist) = ids.anilist.as_ref() {
        entries.push(("anilist", anilist.clone()));
    }
    if let Some(anidb) = ids.anidb.as_ref() {
        entries.push(("anidb", anidb.clone()));
    }
    if let Some(mal) = ids.mal.as_ref() {
        entries.push(("mal", mal.clone()));
    }
    if let Some(kitsu) = ids.kitsu.as_ref() {
        entries.push(("kitsu", kitsu.clone()));
    }

    let stored_count = entries.len();
    let mut inserted_rows = Vec::new();
    for (provider, external_id) in entries {
        let stored_confidence = if provider == "anilist" {
            confidence.unwrap_or(1.0)
        } else {
            1.0
        };
        if source == "anime_match" {
            sqlx::query::<sqlx::Any>(
                "UPDATE season_external_ids SET confidence = $1, source = 'anime_match' \
                 WHERE season_id = $2 AND provider = $3 AND external_id = $4 \
                   AND source IN ('classifier', 'anilist_chain')",
            )
            .bind(stored_confidence)
            .bind(season_id.to_string())
            .bind(provider)
            .bind(&external_id)
            .execute(pool)
            .await?;
        }
        let result = sqlx::query::<sqlx::Any>(
            "INSERT INTO season_external_ids (id, season_id, provider, external_id, confidence, source) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(season_id.to_string())
        .bind(provider)
        .bind(&external_id)
        .bind(stored_confidence)
        .bind(source)
        .execute(pool)
        .await?;
        if result.rows_affected() == 1 {
            inserted_rows.push(PersistedExternalIdentityRow {
                provider: provider.to_string(),
                external_id,
                source: source.to_string(),
            });
        }
    }

    tracing::trace!(
        season_id = %season_id,
        source,
        stored = stored_count,
        "persisted season external ids"
    );
    Ok(inserted_rows)
}

async fn hydrate_anizip_season_context(
    pool: &AnyPool,
    season_id: Uuid,
    mapping: &AniZipMapping,
    artwork: Option<&ArtworkService>,
) -> Result<()> {
    if let Some(title) = preferred_anizip_title(mapping) {
        sqlx::query::<sqlx::Any>(
            "UPDATE seasons SET \
             title = CASE WHEN title IS NULL OR LENGTH(TRIM(title)) = 0 THEN $1 ELSE title END, \
             updated_at = CURRENT_TIMESTAMP WHERE id = $2",
        )
        .bind(title)
        .bind(season_id.to_string())
        .execute(pool)
        .await?;
    }

    let Some(artwork) = artwork else {
        return Ok(());
    };
    let candidates = anizip_artwork_candidates(mapping);
    if candidates.is_empty() {
        return Ok(());
    }
    let stored = artwork
        .upsert_refs(pool, "season", season_id, &candidates)
        .await?;
    if !stored.is_empty() {
        artwork.cache_primary(pool, &stored, &["anizip"]).await?;
    }
    Ok(())
}

fn preferred_anizip_title(mapping: &AniZipMapping) -> Option<&str> {
    for key in ["en", "x-jat", "romaji", "ja", "x-jpn"] {
        if let Some(title) = mapping
            .titles
            .get(key)
            .map(String::as_str)
            .map(str::trim)
            .filter(|title| !title.is_empty())
        {
            return Some(title);
        }
    }
    mapping
        .titles
        .iter()
        .filter_map(|(key, title)| {
            let title = title.trim();
            (!title.is_empty()).then_some((key.as_str(), title))
        })
        .min_by(|left, right| left.0.cmp(right.0))
        .map(|(_, title)| title)
}

fn anizip_artwork_candidates(mapping: &AniZipMapping) -> Vec<ArtworkCandidate> {
    mapping
        .images
        .iter()
        .filter_map(|image| {
            let url = image.url.as_deref()?.trim();
            if url.is_empty() {
                return None;
            }
            let kind = anizip_artwork_kind(image.cover_type.as_deref()?)?;
            Some(ArtworkCandidate {
                kind,
                url: url.to_string(),
                language: None,
                width: None,
                height: None,
                provider: Some("anizip".to_string()),
                score: None,
                metadata_json: Some(serde_json::json!({
                    "coverType": image.cover_type.as_deref(),
                })),
            })
        })
        .collect()
}

fn anizip_artwork_kind(value: &str) -> Option<ArtworkKind> {
    let normalized = value
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '-', '_'], "");
    match normalized.as_str() {
        "poster" | "cover" | "coverart" => Some(ArtworkKind::Poster),
        "fanart" | "background" | "backdrop" => Some(ArtworkKind::Backdrop),
        "banner" => Some(ArtworkKind::Banner),
        "thumbnail" | "thumb" => Some(ArtworkKind::Thumbnail),
        _ => None,
    }
}

async fn ensure_tvdb_season_scaffold(
    pool: &AnyPool,
    series_id: Uuid,
    season_id: Uuid,
    tvdb_series_id: &str,
    season_number: i32,
    linker: &LinkerService,
    artwork: Option<&ArtworkService>,
    ttl_seconds: u64,
    force_metadata: bool,
) -> Result<()> {
    if season_scaffolded_recent(pool, season_id, Some("tvdb"), ttl_seconds, force_metadata).await? {
        return Ok(());
    }
    let episodes = linker
        .fetch_tvdb_season_episodes(tvdb_series_id, season_number)
        .await
        .unwrap_or_default();
    let mut processed = 0usize;
    for episode in episodes {
        let ep_number = match episode.episode_number {
            Some(num) => num,
            None => continue,
        };
        if let Some(ep_season) = episode.season_number {
            if ep_season != season_number {
                continue;
            }
        }
        let episode_id = upsert_episode(
            pool,
            series_id,
            season_id,
            season_number,
            ep_number,
            episode.absolute_number,
        )
        .await?;
        update_episode_details(
            pool,
            episode_id,
            episode.title.as_deref(),
            minutes_to_seconds(episode.runtime_minutes),
            &episode.raw,
        )
        .await?;
        if let (Some(artwork_service), Some(url)) = (artwork, episode.image.as_deref()) {
            sync_episode_artwork(pool, artwork_service, episode_id, url, "tvdb").await?;
        }
        if let Some(tvdb_episode_id) = episode.tvdb_episode_id.as_ref() {
            insert_episode_external_id(pool, episode_id, "tvdb_episode", tvdb_episode_id, "tvdb")
                .await?;
        }
        processed += 1;
    }

    if processed > 0 {
        mark_season_scaffolded(pool, season_id, "tvdb").await?;
    }
    Ok(())
}

async fn ensure_anizip_season_scaffold(
    pool: &AnyPool,
    series_id: Uuid,
    season_id: Uuid,
    season_number: i32,
    mapping: &AniZipMapping,
    artwork: Option<&ArtworkService>,
    ttl_seconds: u64,
    force_metadata: bool,
) -> Result<()> {
    if season_number < 0 {
        return Ok(());
    }
    let refresh_assets =
        !season_scaffolded_recent(pool, season_id, Some("anizip"), ttl_seconds, force_metadata)
            .await?;

    let mut processed = 0usize;
    for episode in mapping.episodes.iter().filter(|ep| {
        ep.season_number
            .map(|num| num == season_number)
            .unwrap_or(false)
    }) {
        let ep_number = match episode.episode_number {
            Some(num) if num > 0 => num,
            None => continue,
            Some(_) => continue,
        };
        let episode_id = upsert_episode(
            pool,
            series_id,
            season_id,
            season_number,
            ep_number,
            episode.absolute_episode_number,
        )
        .await?;
        update_episode_details(
            pool,
            episode_id,
            episode.title.as_deref(),
            minutes_to_seconds(episode.runtime_minutes),
            &episode.raw,
        )
        .await?;
        upsert_anime_episode_meta(pool, season_id, ep_number, episode).await?;
        if refresh_assets
            && let (Some(artwork_service), Some(url)) = (artwork, episode.image.as_deref())
        {
            sync_episode_artwork(pool, artwork_service, episode_id, url, "anizip").await?;
        }
        if let Some(tvdb_id) = episode.tvdb_id.as_ref() {
            insert_episode_external_id(pool, episode_id, "tvdb_episode", tvdb_id, "anizip").await?;
            insert_episode_provider_key(pool, episode_id, "tvdb", tvdb_id).await?;
        }
        if let Some(anidb_eid) = episode.anidb_eid.as_ref() {
            insert_episode_external_id(pool, episode_id, "anidb_episode", anidb_eid, "anizip")
                .await?;
            insert_episode_provider_key(pool, episode_id, "anidb", anidb_eid).await?;
        }
        processed += 1;
    }

    if processed > 0 && refresh_assets {
        mark_season_scaffolded(pool, season_id, "anizip").await?;
    }
    Ok(())
}

async fn update_episode_details(
    pool: &AnyPool,
    episode_id: Uuid,
    title: Option<&str>,
    runtime_seconds: Option<i32>,
    raw: &serde_json::Value,
) -> Result<()> {
    let raw_json = serde_json::to_string(raw).ok();
    sqlx::query::<sqlx::Any>(
        "UPDATE episodes
         SET title = COALESCE($1, NULLIF(TRIM(title), ''), title),
             runtime_seconds = COALESCE($2, runtime_seconds),
             metadata_json = COALESCE($3, NULLIF(TRIM(CAST(metadata_json AS TEXT)), ''), metadata_json),
             updated_at = CURRENT_TIMESTAMP
         WHERE id = $4",
    )
    .bind(title)
    .bind(runtime_seconds)
    .bind(raw_json)
    .bind(episode_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_episode_external_id(
    pool: &AnyPool,
    episode_id: Uuid,
    provider: &str,
    external_id: &str,
    source: &str,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "INSERT INTO episode_external_ids (id, episode_id, provider, external_id, confidence, source) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(episode_id.to_string())
    .bind(provider)
    .bind(external_id)
    .bind(1.0_f32)
    .bind(source)
    .execute(pool)
    .await?;
    Ok(())
}

async fn upsert_anime_episode_meta(
    pool: &AnyPool,
    season_id: Uuid,
    episode_number: i32,
    episode: &AniZipEpisodeRecord,
) -> Result<()> {
    let raw_json = serde_json::to_string(&episode.raw).ok();
    let duration_seconds = minutes_to_seconds(episode.runtime_minutes);
    sqlx::query::<sqlx::Any>(
        "INSERT INTO anime_episode_meta \
         (id, season_id, episode_number, title, snapshot_url, duration_seconds, raw_json, \
          created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) \
         ON CONFLICT(season_id, episode_number) DO UPDATE SET \
         title = COALESCE(excluded.title, anime_episode_meta.title), \
         snapshot_url = COALESCE(excluded.snapshot_url, anime_episode_meta.snapshot_url), \
         duration_seconds = COALESCE( \
             excluded.duration_seconds, anime_episode_meta.duration_seconds \
         ), \
         raw_json = COALESCE(excluded.raw_json, anime_episode_meta.raw_json), \
         updated_at = CURRENT_TIMESTAMP",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(season_id.to_string())
    .bind(episode_number)
    .bind(episode.title.as_deref())
    .bind(episode.image.as_deref())
    .bind(duration_seconds)
    .bind(raw_json)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_episode_provider_key(
    pool: &AnyPool,
    episode_id: Uuid,
    provider: &str,
    provider_key: &str,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "INSERT INTO episode_provider_keys (id, episode_id, provider, provider_key) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(episode_id.to_string())
    .bind(provider)
    .bind(provider_key)
    .execute(pool)
    .await?;
    Ok(())
}

async fn sync_movie_artwork(
    pool: &AnyPool,
    artwork: &ArtworkService,
    movie_id: Uuid,
    meta: Option<&MetadataResult>,
    tvdb_movie_meta: Option<&serde_json::Value>,
) -> Result<()> {
    let mut refs = Vec::new();
    if let Some(tvdb_movie_meta) = tvdb_movie_meta {
        refs.extend(extract_tvdb_entity_artwork(tvdb_movie_meta));
        for entry in extract_tvdb_artworks(tvdb_movie_meta) {
            refs.push(ArtworkCandidate {
                kind: entry.kind,
                url: entry.url,
                language: entry.language,
                width: entry.width,
                height: entry.height,
                provider: Some("tvdb".to_string()),
                score: entry.score,
                metadata_json: None,
            });
        }
    }
    if let Some(meta) = meta {
        refs.extend(extract_cinemeta_artwork(&meta.metadata_json));
    }
    if refs.is_empty() {
        return Ok(());
    }
    let stored = artwork.upsert_refs(pool, "movie", movie_id, &refs).await?;
    artwork
        .cache_primary(pool, &stored, &["tvdb", "cinemeta"])
        .await?;
    Ok(())
}

async fn sync_series_artwork(
    pool: &AnyPool,
    artwork: &ArtworkService,
    series_id: Uuid,
    meta: Option<&MetadataResult>,
    ids: &ExternalIds,
    is_anime: bool,
    linkers: Option<&LinkerService>,
    season_ids: &HashMap<i32, Uuid>,
    ttl_seconds: u64,
    force_metadata: bool,
) -> Result<()> {
    let mut refs = Vec::new();
    if let Some(meta) = meta {
        refs.extend(extract_anilist_artwork(&meta.metadata_json));
        refs.extend(extract_cinemeta_artwork(&meta.metadata_json));
    }

    let mut season_refs: HashMap<i32, Vec<ArtworkCandidate>> = HashMap::new();
    if let (Some(linker), Some(tvdb_id)) = (linkers, ids.tvdb_series.as_ref().or(ids.tvdb.as_ref()))
    {
        let should_refresh =
            !series_seasons_scaffolded_recent(pool, series_id, None, ttl_seconds, force_metadata)
                .await?;
        if should_refresh {
            if let Ok(Some(series_meta)) = linker.fetch_tvdb_series(tvdb_id).await {
                update_series_metadata_from_tvdb(pool, series_id, &series_meta).await?;
                refs.extend(extract_tvdb_series_artwork(&series_meta));
            }
            if let Ok(seasons) = linker.fetch_tvdb_series_seasons(tvdb_id).await {
                update_season_metadata_from_tvdb(pool, &seasons, season_ids).await?;
            }
            if let Ok(Some(artworks_meta)) = linker.fetch_tvdb_series_artworks(tvdb_id).await {
                for entry in extract_tvdb_artworks(&artworks_meta) {
                    let candidate = ArtworkCandidate {
                        kind: entry.kind,
                        url: entry.url,
                        language: entry.language,
                        width: entry.width,
                        height: entry.height,
                        provider: Some("tvdb".to_string()),
                        score: entry.score,
                        metadata_json: None,
                    };
                    if let Some(season_number) = entry.season_number {
                        season_refs
                            .entry(season_number)
                            .or_default()
                            .push(candidate);
                    } else {
                        refs.push(candidate);
                    }
                }
            }
        }
    }

    if !refs.is_empty() {
        let stored = artwork
            .upsert_refs(pool, "series", series_id, &refs)
            .await?;
        if !stored.is_empty() {
            const ANIME_PRIORITY: [&str; 3] = ["anilist", "tvdb", "cinemeta"];
            const SERIES_PRIORITY: [&str; 2] = ["tvdb", "cinemeta"];
            let provider_priority: &[&str] = if is_anime {
                &ANIME_PRIORITY
            } else {
                &SERIES_PRIORITY
            };
            artwork
                .cache_primary(pool, &stored, provider_priority)
                .await?;
        }
    }

    for (season_number, refs) in season_refs {
        let Some(season_id) = season_ids.get(&season_number).copied() else {
            continue;
        };
        let stored = artwork
            .upsert_refs(pool, "season", season_id, &refs)
            .await?;
        if !stored.is_empty() {
            artwork.cache_primary(pool, &stored, &["tvdb"]).await?;
        }
    }

    Ok(())
}

async fn sync_episode_artwork(
    pool: &AnyPool,
    artwork: &ArtworkService,
    episode_id: Uuid,
    url: &str,
    provider: &str,
) -> Result<()> {
    if url.trim().is_empty() {
        return Ok(());
    }
    let candidate = ArtworkCandidate {
        kind: ArtworkKind::Thumbnail,
        url: url.to_string(),
        language: None,
        width: None,
        height: None,
        provider: Some(provider.to_string()),
        score: None,
        metadata_json: None,
    };
    let stored = artwork
        .upsert_refs(pool, "episode", episode_id, &[candidate])
        .await?;
    if !stored.is_empty() {
        artwork
            .cache_primary(pool, &stored, &["tvdb", "anizip"])
            .await?;
    }
    Ok(())
}

async fn update_series_metadata_from_tvdb(
    pool: &AnyPool,
    series_id: Uuid,
    meta: &serde_json::Value,
) -> Result<()> {
    let raw_json = serde_json::to_string(meta).ok();
    let Some(raw_json) = raw_json else {
        return Ok(());
    };
    let existing = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT COALESCE(CAST(metadata_json AS TEXT), '') FROM series WHERE id = $1 LIMIT 1",
    )
    .bind(series_id.to_string())
    .fetch_optional(pool)
    .await?;
    let existing = existing.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    });

    let merged_json = if let Some(existing) = existing {
        let mut merged = serde_json::from_str::<serde_json::Value>(&existing)
            .unwrap_or_else(|_| serde_json::json!({}));
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&raw_json) {
            if let Some(obj) = merged.as_object_mut() {
                obj.insert("tvdb".to_string(), parsed);
            } else {
                merged = serde_json::json!({ "tvdb": parsed });
            }
        }
        serde_json::to_string(&merged).ok()
    } else {
        Some(raw_json)
    };
    let Some(merged_json) = merged_json else {
        return Ok(());
    };

    sqlx::query::<sqlx::Any>(
        "UPDATE series SET metadata_json = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2",
    )
    .bind(merged_json)
    .bind(series_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn update_season_metadata_from_tvdb(
    pool: &AnyPool,
    seasons: &[serde_json::Value],
    season_ids: &HashMap<i32, Uuid>,
) -> Result<()> {
    for season_meta in seasons {
        let Some(season_number) = extract_tvdb_season_number(season_meta) else {
            continue;
        };
        let Some(season_id) = season_ids.get(&season_number).copied() else {
            continue;
        };
        let title = extract_tvdb_season_title(season_meta);
        let raw_json = serde_json::to_string(season_meta).ok();
        let existing = sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT COALESCE(CAST(metadata_json AS TEXT), '') FROM seasons WHERE id = $1 LIMIT 1",
        )
        .bind(season_id.to_string())
        .fetch_optional(pool)
        .await?;
        let existing = existing.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });

        let mut merged = existing
            .and_then(|value| serde_json::from_str::<serde_json::Value>(&value).ok())
            .unwrap_or_else(|| serde_json::json!({}));

        if let Some(obj) = merged.as_object_mut() {
            if let Some(raw_json) = raw_json {
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&raw_json) {
                    obj.insert("tvdb".to_string(), parsed);
                }
            }
        }

        sqlx::query::<sqlx::Any>(
            "UPDATE seasons SET title = COALESCE($1, title), metadata_json = $2, updated_at = CURRENT_TIMESTAMP WHERE id = $3",
        )
        .bind(title.as_deref())
        .bind(serde_json::to_string(&merged).ok())
        .bind(season_id.to_string())
        .execute(pool)
        .await?;
    }
    Ok(())
}

fn extract_tvdb_season_number(value: &serde_json::Value) -> Option<i32> {
    value
        .get("number")
        .or_else(|| value.get("seasonNumber"))
        .or_else(|| value.get("season_number"))
        .and_then(serde_json::Value::as_i64)
        .map(|v| v as i32)
}

fn extract_tvdb_season_title(value: &serde_json::Value) -> Option<String> {
    value
        .get("name")
        .or_else(|| value.get("title"))
        .and_then(serde_json::Value::as_str)
        .map(|s| s.to_string())
        .filter(|s| !s.trim().is_empty())
}

fn extract_tvdb_season_year(value: &serde_json::Value) -> Option<i32> {
    if let Some(year) = value.get("year").and_then(serde_json::Value::as_i64) {
        return Some(year as i32);
    }
    if let Some(year) = value.get("year").and_then(serde_json::Value::as_str) {
        return parse_year_str(year);
    }
    let date_keys = [
        "firstAired",
        "first_air_date",
        "startDate",
        "premiereDate",
        "airDate",
    ];
    for key in date_keys {
        if let Some(value) = value.get(key).and_then(serde_json::Value::as_str) {
            if let Some(year) = value.get(0..4).and_then(|s| s.parse::<i32>().ok()) {
                return Some(year);
            }
        }
    }
    None
}

async fn season_scaffolded_recent(
    pool: &AnyPool,
    season_id: Uuid,
    provider: Option<&str>,
    ttl_seconds: u64,
    force: bool,
) -> Result<bool> {
    if force || ttl_seconds == 0 {
        return Ok(false);
    }

    let row = sqlx::query(
        "SELECT COALESCE(CAST(metadata_json AS TEXT), '') as meta, CAST(updated_at AS TEXT) as updated_at \
         FROM seasons WHERE id = $1 LIMIT 1",
    )
    .bind(season_id.to_string())
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else {
        return Ok(false);
    };

    let meta: String = row.get("meta");
    let meta = meta.trim();
    if meta.is_empty() {
        return Ok(false);
    }
    let parsed: serde_json::Value = serde_json::from_str(meta).unwrap_or_default();
    if !parsed
        .get("scaffolded")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return Ok(false);
    }
    if let Some(expected) = provider {
        let actual = parsed
            .get("scaffold_provider")
            .and_then(serde_json::Value::as_str);
        if actual != Some(expected) {
            return Ok(false);
        }
    }

    let mut timestamp = parsed
        .get("scaffolded_at")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|dt| dt.with_timezone(&Utc));

    if timestamp.is_none() {
        let updated_at: Option<String> = row.try_get("updated_at").ok();
        if let Some(updated_at) = updated_at {
            if let Ok(parsed) = updated_at.parse::<chrono::NaiveDateTime>() {
                timestamp = Some(chrono::DateTime::<Utc>::from_naive_utc_and_offset(
                    parsed, Utc,
                ));
            }
        }
    }

    let Some(timestamp) = timestamp else {
        return Ok(false);
    };
    let age = Utc::now() - timestamp;
    Ok(age.num_seconds() as u64 <= ttl_seconds)
}

async fn series_seasons_scaffolded_recent(
    pool: &AnyPool,
    series_id: Uuid,
    provider: Option<&str>,
    ttl_seconds: u64,
    force: bool,
) -> Result<bool> {
    if force || ttl_seconds == 0 {
        return Ok(false);
    }
    let season_ids: Vec<String> =
        sqlx::query_scalar::<sqlx::Any, String>("SELECT id FROM seasons WHERE series_id = $1")
            .bind(series_id.to_string())
            .fetch_all(pool)
            .await?;

    if season_ids.is_empty() {
        return Ok(false);
    }

    for season_id in season_ids {
        let id = Uuid::parse_str(&season_id)?;
        if !season_scaffolded_recent(pool, id, provider, ttl_seconds, force).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn mark_season_scaffolded(pool: &AnyPool, season_id: Uuid, provider: &str) -> Result<()> {
    let existing = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT COALESCE(CAST(metadata_json AS TEXT), '') FROM seasons WHERE id = $1 LIMIT 1",
    )
    .bind(season_id.to_string())
    .fetch_optional(pool)
    .await?;
    let existing = existing.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    });

    let mut meta = existing
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .unwrap_or_else(|| serde_json::json!({}));

    if let Some(obj) = meta.as_object_mut() {
        obj.insert("scaffolded".to_string(), serde_json::json!(true));
        obj.insert("scaffold_provider".to_string(), serde_json::json!(provider));
        obj.insert(
            "scaffolded_at".to_string(),
            serde_json::json!(Utc::now().to_rfc3339()),
        );
    }

    sqlx::query::<sqlx::Any>(
        "UPDATE seasons SET metadata_json = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2",
    )
    .bind(serde_json::to_string(&meta).ok())
    .bind(season_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

fn minutes_to_seconds(runtime_minutes: Option<i32>) -> Option<i32> {
    runtime_minutes.and_then(|m| if m > 0 { Some(m * 60) } else { None })
}

fn target_episode_runtime_seconds(metadata: &serde_json::Value) -> Option<i32> {
    let direct_seconds = metadata
        .get("runtimeSeconds")
        .or_else(|| metadata.get("durationSeconds"))
        .and_then(serde_json::Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .filter(|value| *value > 0);
    if direct_seconds.is_some() {
        return direct_seconds;
    }

    metadata
        .get("runtime")
        .or_else(|| metadata.get("duration"))
        .and_then(serde_json::Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .filter(|value| *value > 0)
        .map(|value| if value < 1_000 { value * 60 } else { value })
}

#[derive(Debug)]
struct ManagedIdentityLock {
    media_type: MediaType,
    title: String,
    year: Option<i32>,
    external_ids: Option<ExternalIds>,
}

async fn load_managed_identity_lock(
    pool: &AnyPool,
    media_item_id: &str,
) -> Result<Option<ManagedIdentityLock>> {
    let Some(row) = sqlx::query(
        "SELECT media_type, title, year, CAST(external_ids_json AS TEXT) AS external_ids_json
         FROM managed_library_provenance
         WHERE media_item_id = $1
         LIMIT 1",
    )
    .bind(media_item_id)
    .fetch_optional(pool)
    .await?
    else {
        return Ok(None);
    };

    let media_type_raw: String = row.try_get("media_type")?;
    let media_type = match media_type_raw.trim().to_ascii_lowercase().as_str() {
        "movie" => MediaType::Movie,
        "series" => MediaType::Series,
        "anime" => MediaType::Anime,
        _ => MediaType::Series,
    };
    let external_ids = row
        .try_get::<Option<String>, _>("external_ids_json")
        .ok()
        .flatten()
        .and_then(|raw| serde_json::from_str::<ExternalIds>(&raw).ok());
    let year = row.try_get::<Option<i64>, _>("year").ok().flatten();

    Ok(Some(ManagedIdentityLock {
        media_type,
        title: row.try_get("title")?,
        year: year.map(|value| value as i32),
        external_ids,
    }))
}

#[cfg(test)]
mod alm2_tests;

#[cfg(test)]
mod alm8_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        acquisition::subscriptions::{
            AcquisitionMonitorPolicy, AcquisitionRoutePolicy, AcquisitionTargetState,
            NewAcquisitionSubscription, NewAcquisitionTarget, create_subscription,
            upsert_subscription_targets,
        },
        config::{ClassifierConfig, DatabaseConfig, MetadataConfig},
        db::Database,
        db::models::{ExtensionKind, ExtensionTrustLevel, ProviderHealthState, SlotCardinality},
        extensions::{
            ExternalIds as ExtIds, FileDescriptor as FD, MediaIdentity,
            store::{
                ExtensionStore, ManagedImportFile, NewExtension, NewExtensionInstance,
                NewManagedImportEvent, NewManagedIngestIntent, NewManagedMediaTombstone,
                NewProvider,
            },
        },
    };
    use async_trait::async_trait;
    use axum::{
        Json, Router,
        body::Body,
        extract::Path as AxumPath,
        http::StatusCode,
        response::IntoResponse,
        routing::{get, post},
    };
    use elixir_classifier::{
        hint::{FileInput as TestClassifierInput, HintParser},
        identify::{CandidateMatch, KindHint, MatchFeatures},
    };
    use serde_json::Value;
    use std::collections::HashMap;
    use std::sync::{
        Arc, RwLock,
        atomic::{AtomicUsize, Ordering},
    };
    use tempfile::tempdir;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use uuid::Uuid;

    fn sample_identity() -> MediaIdentity {
        MediaIdentity {
            r#type: MediaType::Movie,
            external_ids: ExtIds {
                tmdb: Some("123".to_string()),
                ..Default::default()
            },
            title: "Test Movie".to_string(),
            year: Some(2024),
            season: None,
            episode: None,
        }
    }

    fn anilist_relation_node(id: i32, title: &str, format: Option<&str>) -> AniListRelationNode {
        AniListRelationNode {
            id,
            title: title.to_string(),
            format: format.map(str::to_string),
            season_year: None,
            start_year: None,
            status: Some("FINISHED".to_string()),
            episodes: Some(12),
            next_airing_episode: None,
        }
    }

    fn anilist_seed(id: i32, confidence: f32) -> SeasonAnilistSeed {
        SeasonAnilistSeed {
            anilist_id: id.to_string(),
            confidence,
            causal_paths: BTreeSet::new(),
        }
    }

    #[test]
    fn relation_chain_uses_anilist_ordinals_for_tokyo_ghoul_four_work_chain() {
        let chain = vec![
            anilist_relation_node(20_605, "Tokyo Ghoul", Some("TV")),
            anilist_relation_node(20_850, "Tokyo Ghoul Root A", Some("TV")),
            anilist_relation_node(100_240, "Tokyo Ghoul:re", Some("TV")),
            anilist_relation_node(102_351, "Tokyo Ghoul:re 2nd Season", Some("TV")),
        ];

        // ani.zip/TVDB serializes the final work under season 3. That target
        // numbering must not shift its independent AniList relation identity.
        let expanded = expand_anilist_season_chain_nodes(&chain, 3, &anilist_seed(102_351, 1.0));
        let identity = expanded
            .iter()
            .map(|entry| {
                (
                    entry.season_number,
                    entry.anilist_id.as_str(),
                    entry.title.as_str(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(
            identity,
            vec![
                (1, "20605", "Tokyo Ghoul"),
                (2, "20850", "Tokyo Ghoul Root A"),
                (3, "100240", "Tokyo Ghoul:re"),
                (4, "102351", "Tokyo Ghoul:re 2nd Season"),
            ]
        );

        let mut direct_seed = anilist_seed(102_351, 1.0);
        direct_seed
            .causal_paths
            .insert("Tokyo Ghoul re 2 - 04.mkv".to_string());
        let mut direct_seeds = HashMap::from([(3, direct_seed)]);
        apply_anilist_relation_chain_seeds(&mut direct_seeds, &expanded, &BTreeSet::new());
        assert_eq!(direct_seeds.len(), 4);
        assert_eq!(direct_seeds[&1].anilist_id, "20605");
        assert_eq!(direct_seeds[&2].anilist_id, "20850");
        assert_eq!(direct_seeds[&3].anilist_id, "100240");
        assert_eq!(direct_seeds[&4].anilist_id, "102351");
        assert!(
            direct_seeds[&4]
                .causal_paths
                .contains("Tokyo Ghoul re 2 - 04.mkv")
        );
    }

    #[test]
    fn relation_chain_falls_back_when_known_predecessors_are_missing() {
        let incomplete = vec![
            anilist_relation_node(100_240, "Tokyo Ghoul:re", Some("TV")),
            anilist_relation_node(102_351, "Tokyo Ghoul:re 2nd Season", Some("TV")),
        ];

        assert!(
            expand_anilist_season_chain_nodes(&incomplete, 3, &anilist_seed(102_351, 1.0))
                .is_empty(),
            "a partial chain must retain the caller's deterministic seed instead of inventing S1/S2"
        );
    }

    #[test]
    fn relation_chain_keeps_ova_and_movie_seeds_on_deterministic_fallback() {
        for (id, title, format) in [
            (21_132, "Tokyo Ghoul: JACK", "OVA"),
            (136_430, "Tokyo Ghoul S", "MOVIE"),
        ] {
            let chain = vec![
                anilist_relation_node(20_605, "Tokyo Ghoul", Some("TV")),
                anilist_relation_node(id, title, Some(format)),
            ];
            assert!(
                expand_anilist_season_chain_nodes(&chain, 1, &anilist_seed(id, 1.0)).is_empty(),
                "{format} must not be invented as a numbered season"
            );
        }
    }

    #[test]
    fn relation_identity_and_anizip_target_numbering_remain_independent() {
        let chain = vec![
            anilist_relation_node(20_605, "Tokyo Ghoul", Some("TV")),
            anilist_relation_node(20_850, "Tokyo Ghoul Root A", Some("TV")),
            anilist_relation_node(100_240, "Tokyo Ghoul:re", Some("TV")),
            anilist_relation_node(102_351, "Tokyo Ghoul:re 2nd Season", Some("TV")),
        ];
        let expanded = expand_anilist_season_chain_nodes(&chain, 3, &anilist_seed(102_351, 1.0));
        let target_mapping = AniZipMapping {
            ids: ExtIds {
                anilist: Some("102351".to_string()),
                tvdb_series: Some("305014".to_string()),
                ..Default::default()
            },
            episodes: vec![AniZipEpisodeRecord {
                season_number: Some(3),
                episode_number: Some(16),
                absolute_episode_number: Some(40),
                episode_label: Some("4".to_string()),
                mainline_episode_number: Some(4),
                title: Some("Place".to_string()),
                tvdb_id: Some("7785906".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let direct_target_seeds = HashMap::from([(3, anilist_seed(102_351, 1.0))]);
        let mappings = HashMap::from([("102351".to_string(), Arc::new(target_mapping))]);

        let inputs = library_anime_model_season_inputs(
            "Tokyo Ghoul",
            &expanded,
            &direct_target_seeds,
            &mappings,
        );
        let final_work = inputs
            .iter()
            .filter(|input| input.season.anilist_id == "102351")
            .collect::<Vec<_>>();
        assert_eq!(
            final_work.len(),
            1,
            "the target seed must not duplicate its relation work"
        );
        assert_eq!(final_work[0].season.season_number, 4);

        let targets = build_mapping_targets(
            "Tokyo Ghoul",
            final_work[0].season.season_number,
            final_work[0]
                .mapping
                .as_ref()
                .expect("identity-bound mapping"),
        );
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].target_key, "S04E04");
        assert_eq!(targets[0].season_number, Some(4));
        assert_eq!(targets[0].episode_number, Some(4));
        assert_eq!(targets[0].absolute_episode_number, Some(40));
        assert_eq!(targets[0].tvdb_episode_id.as_deref(), Some("7785906"));

        let root_a_mapping = AniZipMapping {
            episodes: vec![AniZipEpisodeRecord {
                season_number: Some(2),
                episode_number: Some(1),
                absolute_episode_number: Some(13),
                episode_label: Some("1".to_string()),
                mainline_episode_number: Some(1),
                ..Default::default()
            }],
            ..Default::default()
        };
        let root_a_targets = build_mapping_targets("Tokyo Ghoul", 2, &root_a_mapping);
        assert_eq!(root_a_targets[0].target_key, "S02E01");
        assert_eq!(root_a_targets[0].absolute_episode_number, Some(13));
    }

    async fn install_test_managed_provider(
        pool: &AnyPool,
        provider_id: Uuid,
        implementation: &str,
        capability: &str,
    ) -> Result<()> {
        let store = ExtensionStore::new(pool);
        let extension_id = format!("test.manager.{provider_id}");
        let instance_id = Uuid::new_v4();

        store
            .upsert_extension(&NewExtension {
                extension_id: extension_id.clone(),
                name: format!("Test {implementation}"),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Community,
                manifest_json: serde_json::json!({}),
                package_hash: None,
                enabled: true,
            })
            .await?;
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id,
                instance_name: "default".to_string(),
                config_json: Some(serde_json::json!({})),
                enabled: true,
            })
            .await?;
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: capability.to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some(implementation.to_string()),
                scope_json: None,
                endpoint_json: None,
                health_state: ProviderHealthState::Healthy,
            })
            .await?;

        Ok(())
    }

    #[derive(Clone)]
    struct Alm1FixedHintParser {
        hint: ClassifierHint,
    }

    impl HintParser for Alm1FixedHintParser {
        fn name(&self) -> &'static str {
            "alm1_fixed"
        }

        fn parse(&self, input: &TestClassifierInput) -> Vec<ClassifierHint> {
            let mut hint = self.hint.clone();
            hint.source_path = Some(input.path.clone());
            vec![hint]
        }
    }

    struct Alm1PathHintParser {
        hints: HashMap<String, ClassifierHint>,
    }

    impl HintParser for Alm1PathHintParser {
        fn name(&self) -> &'static str {
            "alm1_path"
        }

        fn parse(&self, input: &TestClassifierInput) -> Vec<ClassifierHint> {
            self.hints
                .get(&input.path)
                .cloned()
                .map(|mut hint| {
                    hint.source_path = Some(input.path.clone());
                    vec![hint]
                })
                .unwrap_or_default()
        }
    }

    #[derive(Clone)]
    struct Alm1MutableIdentifier {
        candidates: Arc<RwLock<Vec<CandidateMatch>>>,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl IdentifierProvider for Alm1MutableIdentifier {
        fn name(&self) -> &'static str {
            "alm1_mock"
        }

        fn supports(&self, _library_type: ClassifierLibraryType) -> bool {
            true
        }

        async fn identify(&self, _hint: &ClassifierHint) -> Result<Vec<CandidateMatch>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self
                .candidates
                .read()
                .expect("ALM-1 candidate lock poisoned")
                .clone())
        }
    }

    #[derive(Clone)]
    struct Alm1PathIdentifier {
        candidates: Arc<HashMap<String, Vec<CandidateMatch>>>,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl IdentifierProvider for Alm1PathIdentifier {
        fn name(&self) -> &'static str {
            "alm1_path_mock"
        }

        fn supports(&self, _library_type: ClassifierLibraryType) -> bool {
            true
        }

        async fn identify(&self, hint: &ClassifierHint) -> Result<Vec<CandidateMatch>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(hint
                .source_path
                .as_ref()
                .and_then(|path| self.candidates.get(path))
                .cloned()
                .unwrap_or_default())
        }
    }

    #[derive(Clone)]
    struct Alm1FailingIdentifier {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl IdentifierProvider for Alm1FailingIdentifier {
        fn name(&self) -> &'static str {
            "alm1_failing"
        }

        fn supports(&self, _library_type: ClassifierLibraryType) -> bool {
            true
        }

        async fn identify(&self, _hint: &ClassifierHint) -> Result<Vec<CandidateMatch>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            anyhow::bail!("simulated classifier provider outage")
        }
    }

    fn alm1_classifier_pipeline(
        hint: ClassifierHint,
        candidates: Arc<RwLock<Vec<CandidateMatch>>>,
        calls: Arc<AtomicUsize>,
    ) -> ClassifierPipeline {
        ClassifierPipeline::new()
            .register_hint_parser(Arc::new(Alm1FixedHintParser { hint }))
            .register_identifier_provider(Arc::new(Alm1MutableIdentifier { candidates, calls }))
    }

    fn alm1_production_parser_pipeline(
        candidates: Arc<RwLock<Vec<CandidateMatch>>>,
        calls: Arc<AtomicUsize>,
    ) -> ClassifierPipeline {
        ClassifierPipeline::new()
            .register_hint_parser(Arc::new(GeneralParser::default()))
            .register_hint_parser(Arc::new(AnimeParserAdapter::default()))
            .register_identifier_provider(Arc::new(Alm1MutableIdentifier { candidates, calls }))
    }

    fn alm1_hint(
        library_type: ClassifierLibraryType,
        title: &str,
        season: Option<i32>,
        episode: Option<i32>,
        absolute_episode: Option<i32>,
    ) -> ClassifierHint {
        ClassifierHint {
            library_type,
            title: title.to_string(),
            alt_titles: Vec::new(),
            year: Some(2024),
            season,
            episode,
            absolute_episode,
            duration_seconds: None,
            embedded_ids: ClassifierExternalIds::default(),
            parser: "alm1_fixed",
            parser_confidence: 1.0,
            source_path: None,
        }
    }

    fn alm1_candidate(
        kind: KindHint,
        title: &str,
        tvdb_series: Option<&str>,
        anilist: Option<&str>,
        season: Option<i32>,
        episode: Option<i32>,
        absolute_episode: Option<i32>,
        provider_confidence: f32,
    ) -> CandidateMatch {
        CandidateMatch {
            provider: "alm1_mock",
            kind,
            ids: ClassifierExternalIds {
                tvdb_series: tvdb_series.map(str::to_string),
                anilist: anilist.map(str::to_string),
                ..Default::default()
            },
            input_echo: false,
            title: title.to_string(),
            alt_titles: Vec::new(),
            year: Some(2024),
            season,
            episode,
            absolute_episode,
            duration_seconds: None,
            provider_confidence,
            score: 0.0,
            features: MatchFeatures::default(),
        }
    }

    fn alm1_scan_candidate(
        path: &str,
        media_type: MediaType,
        title: &str,
        year: Option<i32>,
        season: Option<i32>,
        episode: Option<i32>,
    ) -> MediaFileCandidate {
        MediaFileCandidate {
            identity: MediaIdentity {
                r#type: media_type,
                external_ids: ExtIds::default(),
                title: title.to_string(),
                year,
                season,
                episode,
            },
            files: vec![FD {
                path: path.to_string(),
                size_bytes: Some(2_048),
                hash: None,
                container: Some("mkv".to_string()),
                video_codec: Some("h264".to_string()),
                audio_codec: Some("aac".to_string()),
            }],
            extension_metadata: HashMap::new(),
            source_config_id: None,
        }
    }

    #[tokio::test]
    async fn alm8_production_applied_evidence_is_v2_exact_and_rescan_stable() -> Result<()> {
        let database = Database::connect(&DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        })
        .await?;
        database.run_migrations().await?;
        let media_dir = tempdir()?;
        let path = media_dir
            .path()
            .join("Evidence Series S02E01.mkv")
            .to_string_lossy()
            .to_string();
        std::fs::write(&path, b"alm8-exact-evidence")?;
        let calls = Arc::new(AtomicUsize::new(0));
        let pipeline = alm1_classifier_pipeline(
            alm1_hint(
                ClassifierLibraryType::Series,
                "Evidence Series",
                Some(2),
                Some(1),
                None,
            ),
            Arc::new(RwLock::new(vec![alm1_candidate(
                KindHint::Series,
                "Evidence Series",
                Some("exact-tvdb-series"),
                None,
                Some(2),
                Some(1),
                None,
                1.0,
            )])),
            calls.clone(),
        );
        let candidate = || {
            alm1_scan_candidate(
                &path,
                MediaType::Series,
                "Evidence Series",
                Some(2024),
                Some(2),
                Some(1),
            )
        };

        run_full_scan_with_classifier(
            &database.pool,
            None,
            None,
            None,
            None,
            &pipeline,
            vec![candidate()],
            false,
            false,
            false,
        )
        .await?;

        let first: (i64, String) = sqlx::query_as(
            "SELECT crs.applied_identity_version, crs.applied_identity_evidence_json \
             FROM classifier_resolution_state crs JOIN media_files mf \
             ON mf.id = crs.media_file_id WHERE mf.path = $1",
        )
        .bind(&path)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(first.0, 2);
        let evidence: Value = serde_json::from_str(&first.1)?;
        assert_eq!(evidence["schemaVersion"], 2);
        assert_eq!(
            evidence["causalIdentityRows"]["series"],
            serde_json::json!([{
                "provider": "tvdb",
                "externalId": "exact-tvdb-series",
                "source": "classifier",
            }])
        );
        assert_eq!(
            evidence["causalIdentityRows"]["seasons"],
            serde_json::json!([])
        );
        assert_eq!(
            evidence["causalIdentityRows"]["episodes"],
            serde_json::json!([])
        );

        run_full_scan_with_classifier(
            &database.pool,
            None,
            None,
            None,
            None,
            &pipeline,
            vec![candidate()],
            false,
            true,
            false,
        )
        .await?;
        let second: (i64, String) = sqlx::query_as(
            "SELECT crs.applied_identity_version, crs.applied_identity_evidence_json \
             FROM classifier_resolution_state crs JOIN media_files mf \
             ON mf.id = crs.media_file_id WHERE mf.path = $1",
        )
        .bind(&path)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            second, first,
            "an idempotent rescan must retain exact evidence"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[test]
    fn alm8_changed_applied_result_retains_prior_exact_causal_rows() -> Result<()> {
        let existing_evidence = serde_json::json!({
            "schemaVersion": 2,
            "origin": "deterministic_classifier",
            "acceptedNumbers": {
                "seasonNumber": 1,
                "episodeNumber": 1,
                "absoluteEpisodeNumber": 1,
            },
            "causalIdentityRows": {
                "series": [{
                    "provider": "tvdb",
                    "externalId": "prior-tvdb",
                    "source": "classifier",
                }],
                "seasons": [],
                "episodes": [],
            },
            "hint": { "revision": "prior" },
            "candidates": { "revision": "prior" },
        });
        let outcome = ClassificationOutcome {
            disposition: ClassificationDisposition::Applied,
            confidence: Some(0.99),
            hint_json: Some(serde_json::json!({ "revision": "current" }).to_string()),
            candidates_json: Some(serde_json::json!({ "revision": "current" }).to_string()),
            season_scope: Some(2),
            retry_supersedes_applied: false,
            bridge_protected: false,
            parsed_hint: None,
            accepted_numbers: Some(ResolvedEpisodeNumbers {
                season: Some(2),
                episode: Some(1),
                absolute_episode: Some(13),
            }),
            preserve_authoritative_episode_links: false,
            applied_identity_rows: AppliedClassificationIdentityRows {
                series: BTreeSet::from([PersistedExternalIdentityRow {
                    provider: "anilist".to_string(),
                    external_id: "current-anilist".to_string(),
                    source: "classifier".to_string(),
                }]),
                ..Default::default()
            },
        };
        let new_evidence = applied_classification_identity_evidence(&outcome)?
            .expect("changed Applied result must emit v2 evidence");
        let merged = reconcile_idempotent_applied_identity_evidence(
            &outcome,
            &(
                Some(serde_json::json!({ "revision": "prior" }).to_string()),
                Some(serde_json::json!({ "revision": "prior" }).to_string()),
                Some(existing_evidence.to_string()),
            ),
            Some(new_evidence),
        )?
        .expect("changed Applied result must persist the merged envelope");
        let merged: Value = serde_json::from_str(&merged)?;
        let causal_ids = merged["causalIdentityRows"]["series"]
            .as_array()
            .expect("series causal rows")
            .iter()
            .filter_map(|row| row.get("externalId").and_then(Value::as_str))
            .collect::<BTreeSet<_>>();
        assert_eq!(
            causal_ids,
            BTreeSet::from(["current-anilist", "prior-tvdb"])
        );
        assert_eq!(merged["acceptedNumbers"]["seasonNumber"], 2);
        assert_eq!(merged["acceptedNumbers"]["episodeNumber"], 1);
        assert_eq!(merged["acceptedNumbers"]["absoluteEpisodeNumber"], 13);
        assert_eq!(merged["hint"]["revision"], "current");
        Ok(())
    }

    #[test]
    fn alm8_shared_sibling_identity_is_not_attributed_to_either_file() -> Result<()> {
        let outcome = || ClassificationOutcome {
            disposition: ClassificationDisposition::Applied,
            confidence: Some(0.99),
            hint_json: Some("{}".to_string()),
            candidates_json: Some(
                serde_json::json!({
                    "hypotheses": [{
                        "candidate": {
                            "ids": { "anilist": "shared-anilist" },
                            "season": 2
                        }
                    }]
                })
                .to_string(),
            ),
            season_scope: Some(2),
            retry_supersedes_applied: false,
            bridge_protected: false,
            parsed_hint: None,
            accepted_numbers: Some(ResolvedEpisodeNumbers {
                season: Some(2),
                episode: Some(1),
                absolute_episode: Some(13),
            }),
            preserve_authoritative_episode_links: false,
            applied_identity_rows: Default::default(),
        };
        let mut outcomes = HashMap::from([
            ("sibling-a.mkv".to_string(), outcome()),
            ("sibling-b.mkv".to_string(), outcome()),
        ]);
        let inserted = vec![PersistedExternalIdentityRow {
            provider: "anilist".to_string(),
            external_id: "shared-anilist".to_string(),
            source: "classifier".to_string(),
        }];

        attribute_inserted_classification_identity_rows(
            &mut outcomes,
            AppliedIdentityAttributionTarget::Series {
                claimant_season: None,
            },
            &inserted,
            None,
        );
        attribute_inserted_classification_identity_rows(
            &mut outcomes,
            AppliedIdentityAttributionTarget::Season { season_number: 2 },
            &inserted,
            None,
        );

        for sibling in outcomes.values() {
            assert_eq!(
                sibling.applied_identity_rows,
                AppliedClassificationIdentityRows::default()
            );
            let evidence = serde_json::from_str::<Value>(
                &applied_classification_identity_evidence(sibling)?
                    .expect("new applied result evidence"),
            )?;
            assert_eq!(evidence["schemaVersion"], 2);
            assert_eq!(
                evidence["causalIdentityRows"],
                serde_json::json!({ "series": [], "seasons": [], "episodes": [] })
            );
        }

        outcomes.remove("sibling-b.mkv");
        attribute_inserted_classification_identity_rows(
            &mut outcomes,
            AppliedIdentityAttributionTarget::Season { season_number: 2 },
            &inserted,
            None,
        );
        let sole_owner = outcomes
            .get("sibling-a.mkv")
            .expect("remaining unambiguous owner");
        assert_eq!(
            sole_owner.applied_identity_rows.seasons,
            BTreeSet::from([PersistedSeasonExternalIdentityRow {
                season_number: 2,
                provider: "anilist".to_string(),
                external_id: "shared-anilist".to_string(),
                source: "classifier".to_string(),
            }])
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm8_mixed_applied_and_unresolved_files_do_not_persist_provisional_mapping()
    -> Result<()> {
        let database = Database::connect(&DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        })
        .await?;
        database.run_migrations().await?;
        let media_dir = tempdir()?;
        let applied_path = media_dir
            .path()
            .join("Mixed Isolation S01E01.mkv")
            .to_string_lossy()
            .to_string();
        let unresolved_path = media_dir
            .path()
            .join("Mixed Isolation - 13.mkv")
            .to_string_lossy()
            .to_string();
        std::fs::write(&applied_path, b"alm8-applied")?;
        std::fs::write(&unresolved_path, b"alm8-unresolved")?;

        let hints = HashMap::from([
            (
                applied_path.clone(),
                alm1_hint(
                    ClassifierLibraryType::Anime,
                    "Mixed Isolation",
                    Some(1),
                    Some(1),
                    Some(1),
                ),
            ),
            (
                unresolved_path.clone(),
                alm1_hint(
                    ClassifierLibraryType::Anime,
                    "Provisional Noise",
                    None,
                    None,
                    Some(13),
                ),
            ),
        ]);
        let calls = Arc::new(AtomicUsize::new(0));
        let candidates = HashMap::from([
            (
                applied_path.clone(),
                vec![alm1_candidate(
                    KindHint::Anime,
                    "Mixed Isolation",
                    Some("validated-tvdb"),
                    None,
                    Some(1),
                    Some(1),
                    Some(1),
                    1.0,
                )],
            ),
            (
                unresolved_path.clone(),
                vec![alm1_candidate(
                    KindHint::Anime,
                    "Completely Different Anime",
                    Some("provisional-tvdb"),
                    Some("9009"),
                    Some(9),
                    Some(1),
                    Some(13),
                    0.0,
                )],
            ),
        ]);
        let pipeline = ClassifierPipeline::new()
            .register_hint_parser(Arc::new(Alm1PathHintParser { hints }))
            .register_identifier_provider(Arc::new(Alm1PathIdentifier {
                candidates: Arc::new(candidates),
                calls: calls.clone(),
            }));
        persist_cached_anizip_mapping(
            &database.pool,
            "9009",
            &AniZipMapping {
                ids: ExternalIds {
                    anilist: Some("9009".to_string()),
                    tvdb_series: Some("provisional-tvdb".to_string()),
                    ..Default::default()
                },
                episodes: vec![AniZipEpisodeRecord {
                    season_number: Some(9),
                    episode_number: Some(1),
                    absolute_episode_number: Some(13),
                    episode_label: Some("13".to_string()),
                    mainline_episode_number: Some(13),
                    title: Some("Provisional Episode".to_string()),
                    overview: None,
                    runtime_minutes: Some(24),
                    image: None,
                    tvdb_id: Some("provisional-episode-tvdb".to_string()),
                    anidb_eid: Some("provisional-anidb-episode".to_string()),
                    raw: serde_json::json!({
                        "seasonNumber": 9,
                        "episodeNumber": 1,
                        "absoluteEpisodeNumber": 13,
                    }),
                }],
                images: Vec::new(),
                titles: HashMap::from([(
                    "en".to_string(),
                    "Completely Different Anime".to_string(),
                )]),
            },
        )
        .await?;

        run_full_scan_with_classifier(
            &database.pool,
            None,
            None,
            None,
            None,
            &pipeline,
            vec![
                alm1_scan_candidate(
                    &applied_path,
                    MediaType::Anime,
                    "Mixed Isolation",
                    Some(2024),
                    Some(1),
                    Some(1),
                ),
                alm1_scan_candidate(
                    &unresolved_path,
                    MediaType::Anime,
                    "Mixed Isolation",
                    Some(2024),
                    None,
                    None,
                ),
            ],
            false,
            false,
            false,
        )
        .await?;

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let states: Vec<(String, String)> = sqlx::query_as(
            "SELECT mf.path, crs.disposition FROM media_files mf \
             JOIN classifier_resolution_state crs ON crs.media_file_id = mf.id \
             ORDER BY mf.path",
        )
        .fetch_all(&database.pool)
        .await?;
        assert!(states.contains(&(applied_path.clone(), "applied".to_string())));
        assert!(states.contains(&(unresolved_path.clone(), "unresolved".to_string())));

        let linked_paths: Vec<String> = sqlx::query_scalar(
            "SELECT mf.path FROM media_files mf \
             JOIN episode_files ef ON ef.media_file_id = mf.id ORDER BY mf.path",
        )
        .fetch_all(&database.pool)
        .await?;
        assert_eq!(linked_paths, vec![applied_path.clone()]);

        let series_id: String =
            sqlx::query_scalar("SELECT media_item_id FROM media_files WHERE path = $1")
                .bind(&applied_path)
                .fetch_one(&database.pool)
                .await?;
        let series_identity: (Option<String>, Option<String>) = sqlx::query_as(
            "SELECT external_tvdb_series, external_anilist FROM series WHERE id = $1",
        )
        .bind(&series_id)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(series_identity, (Some("validated-tvdb".to_string()), None));
        let series_rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT provider, external_id, source FROM series_external_ids \
             WHERE series_id = $1 ORDER BY provider, external_id, source",
        )
        .bind(&series_id)
        .fetch_all(&database.pool)
        .await?;
        assert_eq!(
            series_rows,
            vec![(
                "tvdb".to_string(),
                "validated-tvdb".to_string(),
                "classifier".to_string(),
            )]
        );
        let season_rows: Vec<i64> = sqlx::query_scalar(
            "SELECT season_number FROM seasons WHERE series_id = $1 ORDER BY season_number",
        )
        .bind(&series_id)
        .fetch_all(&database.pool)
        .await?;
        assert_eq!(season_rows, vec![1]);
        let episode_rows: Vec<(i64, i64)> = sqlx::query_as(
            "SELECT season_number, episode_number FROM episodes \
             WHERE series_id = $1 ORDER BY season_number, episode_number",
        )
        .bind(&series_id)
        .fetch_all(&database.pool)
        .await?;
        assert_eq!(episode_rows, vec![(1, 1)]);
        let provisional_identity_rows: i64 = sqlx::query_scalar(
            "SELECT \
               (SELECT COUNT(*) FROM series_external_ids \
                WHERE external_id IN ('9009', 'provisional-tvdb')) + \
               (SELECT COUNT(*) FROM season_external_ids \
                WHERE external_id IN ('9009', 'provisional-tvdb')) + \
               (SELECT COUNT(*) FROM episode_external_ids \
                WHERE external_id IN \
                  ('provisional-episode-tvdb', 'provisional-anidb-episode'))",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(provisional_identity_rows, 0);

        Ok(())
    }

    #[test]
    fn tvdb_description_uses_primary_translation_when_top_level_overview_is_missing() {
        let meta = serde_json::json!({
            "translations": {
                "overviewTranslations": [
                    { "language": "deu", "overview": "Deutsche Beschreibung" },
                    {
                        "isPrimary": true,
                        "language": "eng",
                        "overview": "English TVDB description."
                    }
                ]
            }
        });

        assert_eq!(
            extract_tvdb_description(&meta).as_deref(),
            Some("English TVDB description.")
        );
    }

    async fn start_mock_cinemeta_server() -> Result<(String, oneshot::Sender<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let base_url = format!("http://127.0.0.1:{}", addr.port());
        let metadata_base_url = base_url.clone();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let app = Router::new()
            .route(
                "/meta/movie/tt0381061.json",
                get(move || {
                    let metadata_base_url = metadata_base_url.clone();
                    async move {
                        Json(serde_json::json!({
                            "meta": {
                                "imdb_id": "tt0381061",
                                "runtime": "144 min",
                                "description": "A managed Casino Royale description.",
                                "genre": ["Action", "Thriller"],
                                "genres": ["Action", "Thriller"],
                                "poster": format!("{metadata_base_url}/poster.jpg"),
                                "background": format!("{metadata_base_url}/backdrop.jpg")
                            }
                        }))
                    }
                }),
            )
            .route(
                "/poster.jpg",
                get(|| async {
                    (StatusCode::OK, Body::from(vec![0xff, 0xd8, 0xff, 0xd9])).into_response()
                }),
            )
            .route(
                "/backdrop.jpg",
                get(|| async {
                    (StatusCode::OK, Body::from(vec![0xff, 0xd8, 0xff, 0xd9])).into_response()
                }),
            );

        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        Ok((base_url, shutdown_tx))
    }

    async fn start_mock_tvdb_movie_server() -> Result<(String, oneshot::Sender<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let base_url = format!("http://127.0.0.1:{}", addr.port());
        let movie_base_url = base_url.clone();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let app = Router::new()
            .route(
                "/login",
                post(|| async { Json(serde_json::json!({ "data": { "token": "test-token" } })) }),
            )
            .route(
                "/search/remoteid/:imdb",
                get(|AxumPath(imdb): AxumPath<String>| async move {
                    let data = if imdb == "tt0381061" {
                        serde_json::json!([
                            {
                                "id": 76543,
                                "type": "series",
                                "series": { "id": 76543 }
                            },
                            {
                                "id": 12345,
                                "type": "movie",
                                "movie": { "id": 12345 }
                            }
                        ])
                    } else {
                        serde_json::json!([])
                    };
                    Json(serde_json::json!({ "data": data }))
                }),
            )
            .route(
                "/movies/:id/extended",
                get(move |AxumPath(id): AxumPath<String>| {
                    let movie_base_url = movie_base_url.clone();
                    async move {
                        let data = if id == "12345" {
                            serde_json::json!({
                                "id": 12345,
                                "name": "Casino Royale",
                                "year": "2006",
                                "runtime": 144,
                                "overview": "TVDB Casino Royale description.",
                                "image": format!("{movie_base_url}/tvdb-poster.jpg"),
                                "remoteIds": [
                                    { "sourceName": "IMDB", "id": "tt0381061" },
                                    { "sourceName": "TheMovieDB.com", "id": "36557" }
                                ],
                                "genres": [
                                    { "name": "Action" },
                                    { "name": "Thriller" }
                                ],
                                "artworks": [
                                    {
                                        "image": format!("{movie_base_url}/tvdb-poster.jpg"),
                                        "width": 680,
                                        "height": 1000,
                                        "score": 9.1,
                                        "language": "eng"
                                    },
                                    {
                                        "image": format!("{movie_base_url}/tvdb-backdrop.jpg"),
                                        "width": 1920,
                                        "height": 1080,
                                        "score": 8.7,
                                        "language": "eng"
                                    }
                                ]
                            })
                        } else {
                            serde_json::json!(null)
                        };
                        Json(serde_json::json!({ "data": data }))
                    }
                }),
            )
            .route(
                "/tvdb-poster.jpg",
                get(|| async {
                    (StatusCode::OK, Body::from(vec![0xff, 0xd8, 0xff, 0xd9])).into_response()
                }),
            )
            .route(
                "/tvdb-backdrop.jpg",
                get(|| async {
                    (StatusCode::OK, Body::from(vec![0xff, 0xd8, 0xff, 0xd9])).into_response()
                }),
            );

        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        Ok((base_url, shutdown_tx))
    }

    async fn start_mock_tvdb_series_artwork_server() -> Result<(String, oneshot::Sender<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let base_url = format!("http://127.0.0.1:{}", addr.port());
        let series_base_url = base_url.clone();
        let artwork_base_url = base_url.clone();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let app = Router::new()
            .route(
                "/login",
                post(|| async { Json(serde_json::json!({ "data": { "token": "test-token" } })) }),
            )
            .route(
                "/series/:id",
                get(move |AxumPath(id): AxumPath<String>| {
                    let series_base_url = series_base_url.clone();
                    async move {
                        let data = if id == "72244" {
                            serde_json::json!({
                                "id": 72244,
                                "name": "Star Wars: Clone Wars",
                                "year": "2003",
                                "overview": "Animated micro-series following the Clone Wars.",
                                "image": format!("{series_base_url}/tvdb-series-poster.jpg"),
                                "banner": format!("{series_base_url}/tvdb-series-banner.jpg")
                            })
                        } else {
                            serde_json::json!(null)
                        };
                        Json(serde_json::json!({ "data": data }))
                    }
                }),
            )
            .route(
                "/series/:id/extended",
                get(|AxumPath(id): AxumPath<String>| async move {
                    let data = if id == "72244" {
                        serde_json::json!({
                            "id": 72244,
                            "seasons": [
                                { "id": 1, "number": 1, "name": "Season 1" }
                            ]
                        })
                    } else {
                        serde_json::json!({ "id": id, "seasons": [] })
                    };
                    Json(serde_json::json!({ "data": data }))
                }),
            )
            .route(
                "/series/:id/artworks",
                get(move |AxumPath(id): AxumPath<String>| {
                    let artwork_base_url = artwork_base_url.clone();
                    async move {
                        let data = if id == "72244" {
                            serde_json::json!([
                                {
                                    "image": format!("{artwork_base_url}/tvdb-series-poster.jpg"),
                                    "typeName": "Poster",
                                    "language": "eng",
                                    "width": 680,
                                    "height": 1000,
                                    "score": 9.3
                                },
                                {
                                    "image": format!("{artwork_base_url}/tvdb-series-backdrop.jpg"),
                                    "typeName": "Background",
                                    "language": "eng",
                                    "width": 1920,
                                    "height": 1080,
                                    "score": 8.8
                                },
                                {
                                    "image": format!("{artwork_base_url}/tvdb-series-banner.jpg"),
                                    "typeName": "Banner",
                                    "language": "eng",
                                    "width": 758,
                                    "height": 140,
                                    "score": 8.4
                                }
                            ])
                        } else {
                            serde_json::json!([])
                        };
                        Json(serde_json::json!({ "data": data }))
                    }
                }),
            )
            .route(
                "/tvdb-series-poster.jpg",
                get(|| async {
                    (StatusCode::OK, Body::from(vec![0xff, 0xd8, 0xff, 0xd9])).into_response()
                }),
            )
            .route(
                "/tvdb-series-banner.jpg",
                get(|| async {
                    (StatusCode::OK, Body::from(vec![0xff, 0xd8, 0xff, 0xd9])).into_response()
                }),
            )
            .route(
                "/tvdb-series-backdrop.jpg",
                get(|| async {
                    (StatusCode::OK, Body::from(vec![0xff, 0xd8, 0xff, 0xd9])).into_response()
                }),
            );

        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        Ok((base_url, shutdown_tx))
    }

    #[tokio::test]
    async fn upsert_and_mark_missing() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;

        // Initial scan with one file
        let candidates = vec![MediaFileCandidate {
            identity: sample_identity(),
            files: vec![FD {
                path: "/media/movie.mkv".to_string(),
                size_bytes: Some(1024),
                hash: None,
                container: Some("mkv".to_string()),
                video_codec: Some("h264".to_string()),
                audio_codec: Some("aac".to_string()),
            }],
            extension_metadata: HashMap::new(),
            source_config_id: None,
        }];
        run_full_scan(&database.pool, candidates, false).await?;

        let (movie_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM movies")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(movie_count, 1);

        let (link_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM movie_files")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(link_count, 1);

        let owner = sqlx::query(
            "SELECT owner_type, owner_label, release_capability, release_policy
             FROM media_ownerships
             LIMIT 1",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(owner.get::<String, _>("owner_type"), "external");
        let owner_label: Option<String> = owner.try_get("owner_label").ok();
        assert_eq!(owner_label.as_deref(), Some("External import"));
        assert_eq!(owner.get::<String, _>("release_capability"), "none");
        assert_eq!(owner.get::<String, _>("release_policy"), "unsupported");

        let (count_ok,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM media_files WHERE scan_state = 'ok'")
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(count_ok, 1);

        // Second scan with no files should mark missing
        run_full_scan(&database.pool, Vec::new(), false).await?;
        let (count_missing,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM media_files WHERE scan_state = 'missing'")
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(count_missing, 1);

        Ok(())
    }

    #[tokio::test]
    async fn scan_missing_pass_preserves_existing_unseen_files() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let temp = tempdir()?;
        let scan_path = temp.path().join("media").join("visible.mkv");
        let imported_path = temp.path().join("downloads").join("imported.mkv");
        std::fs::create_dir_all(scan_path.parent().unwrap())?;
        std::fs::create_dir_all(imported_path.parent().unwrap())?;
        std::fs::write(&scan_path, b"not-real-video")?;
        std::fs::write(&imported_path, b"not-real-video")?;

        let scan_path = scan_path.to_string_lossy().to_string();
        let imported_path = imported_path.to_string_lossy().to_string();
        let candidate_for = |path: &str, title: &str, tmdb: &str| MediaFileCandidate {
            identity: MediaIdentity {
                r#type: MediaType::Movie,
                external_ids: ExtIds {
                    tmdb: Some(tmdb.to_string()),
                    ..Default::default()
                },
                title: title.to_string(),
                year: Some(2024),
                season: None,
                episode: None,
            },
            files: vec![FD {
                path: path.to_string(),
                size_bytes: Some(1024),
                hash: None,
                container: Some("mkv".to_string()),
                video_codec: Some("h264".to_string()),
                audio_codec: Some("aac".to_string()),
            }],
            extension_metadata: HashMap::new(),
            source_config_id: None,
        };

        run_full_scan(
            &database.pool,
            vec![
                candidate_for(&scan_path, "Visible Movie", "100"),
                candidate_for(&imported_path, "Imported Movie", "200"),
            ],
            false,
        )
        .await?;

        run_full_scan(
            &database.pool,
            vec![candidate_for(&scan_path, "Visible Movie", "100")],
            false,
        )
        .await?;

        let imported_state: String =
            sqlx::query_scalar("SELECT scan_state FROM media_files WHERE path = $1")
                .bind(&imported_path)
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(imported_state, "ok");

        Ok(())
    }

    #[tokio::test]
    async fn corrected_movie_identity_relinks_existing_file() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let path = "/media/movies/Casino Royale (2006)/Casino Royale 2006 BluRay 1080p DDP 5 1 x264-hallowed.mkv";

        let first_scan = vec![MediaFileCandidate {
            identity: MediaIdentity {
                r#type: MediaType::Movie,
                external_ids: ExtIds::default(),
                title: "Casino Royale DDP 5 1 hallowed".to_string(),
                year: None,
                season: None,
                episode: None,
            },
            files: vec![FD {
                path: path.to_string(),
                size_bytes: Some(2048),
                hash: None,
                container: Some("mkv".to_string()),
                video_codec: Some("h264".to_string()),
                audio_codec: Some("aac".to_string()),
            }],
            extension_metadata: HashMap::new(),
            source_config_id: None,
        }];
        run_full_scan(&database.pool, first_scan, false).await?;

        let corrected_scan = vec![MediaFileCandidate {
            identity: MediaIdentity {
                r#type: MediaType::Movie,
                external_ids: ExtIds::default(),
                title: "Casino Royale".to_string(),
                year: Some(2006),
                season: None,
                episode: None,
            },
            files: vec![FD {
                path: path.to_string(),
                size_bytes: Some(2048),
                hash: None,
                container: Some("mkv".to_string()),
                video_codec: Some("h264".to_string()),
                audio_codec: Some("aac".to_string()),
            }],
            extension_metadata: HashMap::new(),
            source_config_id: None,
        }];
        run_full_scan(&database.pool, corrected_scan, false).await?;

        let (movie_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM movies")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(movie_count, 1);
        let (link_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM movie_files")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(link_count, 1);
        let title: String = sqlx::query_scalar("SELECT title FROM movies LIMIT 1")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(title, "Casino Royale");

        Ok(())
    }

    #[tokio::test]
    async fn classifier_persists_internal_retry_state_without_manual_review() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;

        let candidates = vec![MediaFileCandidate {
            identity: MediaIdentity {
                r#type: MediaType::Movie,
                external_ids: ExtIds::default(),
                title: "Review Needed".to_string(),
                year: Some(2024),
                season: None,
                episode: None,
            },
            files: vec![FD {
                path: "/media/review_needed.mkv".to_string(),
                size_bytes: Some(2048),
                hash: None,
                container: Some("mkv".to_string()),
                video_codec: Some("h264".to_string()),
                audio_codec: Some("aac".to_string()),
            }],
            extension_metadata: HashMap::new(),
            source_config_id: None,
        }];
        run_full_scan(&database.pool, candidates, false).await?;

        let (queue_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM review_queue")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(queue_count, 0);
        let disposition: String =
            sqlx::query_scalar("SELECT disposition FROM classifier_resolution_state LIMIT 1")
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(disposition, "unresolved");

        Ok(())
    }

    #[tokio::test]
    async fn classifier_applies_override_during_ingest() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;

        let media_path = "/media/override_movie_2024.mkv";
        let normalized = derive_override_key("movie", media_path).expect("override key");

        sqlx::query(
            "INSERT INTO classifier_overrides (id, library_type, normalized_key, imdb_id, anilist_id, tvdb_id) VALUES ($1, $2, $3, $4, NULL, NULL)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind("movie")
        .bind(normalized)
        .bind("tt9999999")
        .execute(&database.pool)
        .await?;

        let candidates = vec![MediaFileCandidate {
            identity: MediaIdentity {
                r#type: MediaType::Movie,
                external_ids: ExtIds::default(),
                title: "Override Movie".to_string(),
                year: Some(2024),
                season: None,
                episode: None,
            },
            files: vec![FD {
                path: media_path.to_string(),
                size_bytes: Some(2048),
                hash: None,
                container: Some("mkv".to_string()),
                video_codec: Some("h264".to_string()),
                audio_codec: Some("aac".to_string()),
            }],
            extension_metadata: HashMap::new(),
            source_config_id: None,
        }];
        run_full_scan(&database.pool, candidates, false).await?;

        let imdb: Option<String> = sqlx::query_scalar("SELECT external_imdb FROM movies LIMIT 1")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(imdb.as_deref(), Some("tt9999999"));

        let (pending_count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM review_queue WHERE status = 'pending'")
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(pending_count, 0);

        Ok(())
    }

    #[tokio::test]
    async fn alm4_manual_override_is_authoritative_over_derived_bridge_failure() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;

        let media_path = "/media/Tokyo Ghoul Root A - S02E01.mkv";
        let normalized = derive_override_key("anime", media_path).expect("override key");
        sqlx::query(
            "INSERT INTO classifier_overrides \
             (id, library_type, normalized_key, imdb_id, anilist_id, tvdb_id) \
             VALUES ($1, $2, $3, NULL, NULL, $4)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind("anime")
        .bind(normalized)
        .bind("305014")
        .execute(&database.pool)
        .await?;

        let (mut candidates, _) = merge_candidates(
            vec![alm1_scan_candidate(
                media_path,
                MediaType::Anime,
                "Tokyo Ghoul Root A",
                Some(2015),
                Some(2),
                Some(1),
            )],
            false,
        );
        let candidate = candidates.pop().expect("aggregated candidate");
        let classifier = ClassifierPipeline::new();
        let (_, mut outcomes, _, _, _) = classify_candidate_files(
            &database.pool,
            &classifier,
            &candidate,
            &ExternalIds::default(),
            false,
            false,
            false,
        )
        .await?;

        let overridden = outcomes.get(media_path).expect("override outcome");
        assert_eq!(overridden.disposition, ClassificationDisposition::Applied);
        assert!(overridden.bridge_protected);

        apply_tvdb_anime_bridge_outcome(
            &mut outcomes,
            2,
            ClassificationDisposition::Unresolved,
            Some(0.92),
            None,
            Some(serde_json::json!({"winnerMargin": 0.02}).to_string()),
        );

        let protected = outcomes.get(media_path).expect("protected outcome");
        assert_eq!(protected.disposition, ClassificationDisposition::Applied);
        assert!(!protected.retry_supersedes_applied);

        Ok(())
    }

    #[test]
    fn alm1_alm4_disposition_requires_threshold_and_global_winner_margin() {
        let hint_a = alm1_hint(
            ClassifierLibraryType::Series,
            "Tokyo Ghoul",
            Some(2),
            Some(1),
            Some(13),
        );
        let hint_b = ClassifierHint {
            parser: "alm1_competing",
            ..hint_a.clone()
        };
        let scored_candidate = |id: &str, score: f32| {
            let mut candidate = alm1_candidate(
                KindHint::Series,
                "Tokyo Ghoul",
                Some(id),
                None,
                Some(2),
                Some(1),
                Some(13),
                1.0,
            );
            candidate.score = score;
            candidate
        };
        let canonical = |score: f32, considered: Vec<CandidateMatch>| ClassifierCanonicalMatch {
            kind: KindHint::Series,
            ids: considered
                .first()
                .map(|candidate| candidate.ids.clone())
                .unwrap_or_default(),
            season: Some(2),
            episode: Some(1),
            absolute_episode: Some(13),
            confidence: score,
            chosen_provider: "alm1_mock",
            considered,
        };

        let near_tie = select_best_classification(vec![
            ClassifiedHint {
                hint: hint_a.clone(),
                canonical: Some(canonical(
                    0.90,
                    vec![
                        scored_candidate("winner", 0.90),
                        scored_candidate("same-hint-runner-up", 0.87),
                    ],
                )),
            },
            ClassifiedHint {
                hint: hint_b.clone(),
                canonical: Some(canonical(
                    0.89,
                    vec![scored_candidate("other-hint-runner-up", 0.89)],
                )),
            },
        ])
        .expect("near-tie selection");
        assert_eq!(near_tie.runner_up_confidence, Some(0.89));
        assert!(
            near_tie
                .winner_margin
                .is_some_and(|margin| margin < CLASSIFICATION_APPLICATION_MIN_MARGIN)
        );
        assert_eq!(
            classification_disposition(near_tie.canonical.as_ref(), near_tie.winner_margin),
            ClassificationDisposition::Unresolved
        );

        let clear = select_best_classification(vec![
            ClassifiedHint {
                hint: hint_a.clone(),
                canonical: Some(canonical(
                    0.91,
                    vec![scored_candidate("clear-winner", 0.91)],
                )),
            },
            ClassifiedHint {
                hint: hint_b.clone(),
                canonical: Some(canonical(
                    0.80,
                    vec![scored_candidate("clear-runner-up", 0.80)],
                )),
            },
        ])
        .expect("clear selection");
        assert_eq!(
            classification_disposition(clear.canonical.as_ref(), clear.winner_margin),
            ClassificationDisposition::Applied
        );

        let duplicate_identity = select_best_classification(vec![
            ClassifiedHint {
                hint: hint_a.clone(),
                canonical: Some(canonical(
                    0.92,
                    vec![scored_candidate("same-identity", 0.92)],
                )),
            },
            ClassifiedHint {
                hint: hint_b.clone(),
                canonical: Some(canonical(
                    0.91,
                    vec![scored_candidate("same-identity", 0.91)],
                )),
            },
        ])
        .expect("duplicate identity selection");
        assert_eq!(duplicate_identity.runner_up_confidence, None);
        assert_eq!(duplicate_identity.hypotheses.len(), 2);
        assert_eq!(
            classification_disposition(
                duplicate_identity.canonical.as_ref(),
                duplicate_identity.winner_margin,
            ),
            ClassificationDisposition::Applied
        );

        let mut season_two = scored_candidate("same-identity", 0.92);
        season_two.season = None;
        let mut season_three = scored_candidate("same-identity", 0.91);
        season_three.season = None;
        let different_numbering = select_best_classification(vec![
            ClassifiedHint {
                hint: hint_a,
                canonical: Some(canonical(0.92, vec![season_two])),
            },
            ClassifiedHint {
                hint: ClassifierHint {
                    season: Some(3),
                    parser: "alm1_other_season",
                    ..hint_b
                },
                canonical: Some(canonical(0.91, vec![season_three])),
            },
        ])
        .expect("different numbering selection");
        assert_eq!(different_numbering.runner_up_confidence, Some(0.91));
        assert_eq!(
            classification_disposition(
                different_numbering.canonical.as_ref(),
                different_numbering.winner_margin,
            ),
            ClassificationDisposition::Unresolved
        );

        let single = canonical(
            CLASSIFICATION_APPLICATION_CONFIDENCE,
            vec![scored_candidate(
                "single",
                CLASSIFICATION_APPLICATION_CONFIDENCE,
            )],
        );
        assert_eq!(
            classification_disposition(Some(&single), None),
            ClassificationDisposition::Applied
        );
        let below_threshold = ClassifierCanonicalMatch {
            confidence: CLASSIFICATION_APPLICATION_CONFIDENCE - 0.001,
            ..single
        };
        assert_eq!(
            classification_disposition(Some(&below_threshold), None),
            ClassificationDisposition::Unresolved
        );
    }

    #[test]
    fn alm4_production_general_and_anime_hints_compete_under_one_global_margin() {
        let file_name = "Tokyo.Ghoul.Root.A.S02E01.1080p.WEB-DL.Dual.Audio.mkv";
        let mut input = TestClassifierInput::new(file_name);
        input.file_name = Some(file_name.to_string());
        input.library_type_hint = Some(ClassifierLibraryType::Series);

        let general_hint = GeneralParser::default()
            .parse(&input)
            .into_iter()
            .find(|hint| hint.season == Some(2) && hint.episode == Some(1))
            .expect("general parser hint");
        let anime_hint = AnimeParserAdapter::default()
            .parse(&input)
            .into_iter()
            .find(|hint| hint.season == Some(2) && hint.episode == Some(1))
            .expect("anime parser hint");
        assert_eq!(general_hint.library_type, ClassifierLibraryType::Series);
        assert_eq!(anime_hint.library_type, ClassifierLibraryType::Anime);

        let scorer = DefaultScorer::default();
        let general_candidate = alm1_candidate(
            KindHint::Series,
            &general_hint.title,
            Some("305014"),
            None,
            Some(2),
            Some(1),
            None,
            1.0,
        );
        let anime_candidate = alm1_candidate(
            KindHint::Anime,
            &anime_hint.title,
            None,
            Some("20850"),
            Some(2),
            Some(1),
            None,
            1.0,
        );
        let near_tie = select_best_classification(vec![
            ClassifiedHint {
                canonical: scorer.score(&general_hint, &[general_candidate]),
                hint: general_hint.clone(),
            },
            ClassifiedHint {
                canonical: scorer.score(&anime_hint, &[anime_candidate]),
                hint: anime_hint.clone(),
            },
        ])
        .expect("global parser selection");

        assert_eq!(near_tie.hypotheses.len(), 2);
        assert!(near_tie.runner_up_confidence.is_some());
        assert!(
            near_tie
                .winner_margin
                .is_some_and(|margin| margin < CLASSIFICATION_APPLICATION_MIN_MARGIN)
        );
        assert_eq!(
            classification_disposition(near_tie.canonical.as_ref(), near_tie.winner_margin),
            ClassificationDisposition::Unresolved
        );

        let weak_general_candidate = alm1_candidate(
            KindHint::Series,
            "A Completely Different Series",
            Some("999999"),
            None,
            Some(2),
            Some(1),
            None,
            0.7,
        );
        let clear_winner_candidate = alm1_candidate(
            KindHint::Anime,
            &anime_hint.title,
            None,
            Some("20850"),
            Some(2),
            Some(1),
            None,
            1.0,
        );
        let clear = select_best_classification(vec![
            ClassifiedHint {
                canonical: scorer.score(&general_hint, &[weak_general_candidate]),
                hint: general_hint,
            },
            ClassifiedHint {
                canonical: scorer.score(&anime_hint, &[clear_winner_candidate]),
                hint: anime_hint,
            },
        ])
        .expect("clear global parser selection");

        assert!(
            clear
                .winner_margin
                .is_some_and(|margin| margin >= CLASSIFICATION_APPLICATION_MIN_MARGIN)
        );
        assert_eq!(
            classification_disposition(clear.canonical.as_ref(), clear.winner_margin),
            ClassificationDisposition::Applied
        );
        assert_eq!(
            clear
                .canonical
                .as_ref()
                .and_then(|canonical| canonical.ids.anilist.as_deref()),
            Some("20850")
        );
    }

    #[test]
    fn alm4_tvdb_to_anilist_bridge_near_tie_is_retryable_and_clear_winner_applies() -> Result<()> {
        let hint = ClassifierHint {
            parser: "tvdb_bridge",
            ..alm1_hint(
                ClassifierLibraryType::Anime,
                "Tokyo Ghoul Root A",
                Some(2),
                Some(1),
                Some(13),
            )
        };
        let scored_candidate = |id: &str, score: f32| {
            let mut candidate = alm1_candidate(
                KindHint::Anime,
                "Tokyo Ghoul Root A",
                None,
                Some(id),
                Some(2),
                None,
                None,
                1.0,
            );
            candidate.score = score;
            candidate
        };
        let canonical = |score: f32, candidate: CandidateMatch| ClassifierCanonicalMatch {
            kind: KindHint::Anime,
            ids: candidate.ids.clone(),
            season: candidate.season,
            episode: candidate.episode,
            absolute_episode: candidate.absolute_episode,
            confidence: score,
            chosen_provider: "anilist",
            considered: vec![candidate],
        };
        let selection_for = |winner: f32, runner_up: f32| {
            select_best_classification(vec![
                ClassifiedHint {
                    hint: hint.clone(),
                    canonical: Some(canonical(winner, scored_candidate("20850", winner))),
                },
                ClassifiedHint {
                    hint: ClassifierHint {
                        parser: "tvdb_bridge_alias",
                        ..hint.clone()
                    },
                    canonical: Some(canonical(runner_up, scored_candidate("22319", runner_up))),
                },
            ])
            .expect("bridge selection")
        };
        let seed_outcome = || ClassificationOutcome {
            disposition: ClassificationDisposition::Applied,
            confidence: Some(0.95),
            hint_json: None,
            candidates_json: Some(
                serde_json::json!({
                    "hypotheses": [{"candidate": {"ids": {"tvdbSeries": "305014"}}, "score": 0.95}],
                    "runnerUpConfidence": null,
                    "winnerMargin": null,
                })
                .to_string(),
            ),
            season_scope: Some(2),
            retry_supersedes_applied: false,
            bridge_protected: false,
            parsed_hint: Some(hint.clone()),
            accepted_numbers: Some(ResolvedEpisodeNumbers {
                season: Some(2),
                episode: Some(1),
                absolute_episode: Some(13),
            }),
            preserve_authoritative_episode_links: false,
            applied_identity_rows: Default::default(),
        };

        let near_tie = selection_for(0.92, 0.90);
        let near_tie_decision =
            tvdb_anime_bridge_disposition(near_tie.canonical.as_ref(), near_tie.winner_margin, 2);
        assert_eq!(near_tie_decision, ClassificationDisposition::Unresolved);
        let (hint_json, candidates_json) = build_classification_evidence_payloads(
            &near_tie.hint,
            &near_tie.hypotheses,
            near_tie.runner_up_confidence,
            near_tie.winner_margin,
        )?;
        let mut season_three_outcome = seed_outcome();
        season_three_outcome.parsed_hint = None;
        season_three_outcome.accepted_numbers = None;
        season_three_outcome.season_scope = Some(3);
        let mut unresolved_sibling = seed_outcome();
        unresolved_sibling.disposition = ClassificationDisposition::Unresolved;
        unresolved_sibling.parsed_hint = None;
        unresolved_sibling.accepted_numbers = None;
        let mut outcomes = HashMap::from([
            ("root-a.mkv".to_string(), seed_outcome()),
            ("re.mkv".to_string(), season_three_outcome),
            ("unresolved-sibling.mkv".to_string(), unresolved_sibling),
        ]);
        apply_tvdb_anime_bridge_outcome(
            &mut outcomes,
            2,
            near_tie_decision,
            near_tie.canonical.as_ref().map(|value| value.confidence),
            hint_json,
            candidates_json,
        );
        let unresolved = outcomes.get("root-a.mkv").expect("bridge outcome");
        assert_eq!(
            unresolved.disposition,
            ClassificationDisposition::Unresolved
        );
        assert!(
            unresolved.accepted_numbers.is_some(),
            "retry context is retained"
        );
        assert!(
            unresolved.parsed_hint.is_some(),
            "retry context is retained"
        );
        let unresolved_file = AggregatedFile {
            descriptor: FD {
                path: "root-a.mkv".to_string(),
                size_bytes: None,
                hash: None,
                container: None,
                video_codec: None,
                audio_codec: None,
            },
            source_config_id: None,
            extension_metadata: HashMap::new(),
            season: Some(2),
            episode: Some(1),
            absolute_episode: Some(13),
        };
        let inert_numbers = episode_number_evidence(&unresolved_file, Some(unresolved));
        assert_eq!(inert_numbers.season, None);
        assert_eq!(inert_numbers.episode, None);
        assert_eq!(inert_numbers.absolute_episode, None);
        assert_eq!(
            outcomes
                .get("re.mkv")
                .expect("other season outcome")
                .disposition,
            ClassificationDisposition::Applied,
            "a season-two bridge decision must not mutate season three"
        );
        assert_eq!(
            outcomes
                .get("unresolved-sibling.mkv")
                .expect("unresolved sibling")
                .disposition,
            ClassificationDisposition::Unresolved,
            "a bridge decision must never promote a file that failed its own classifier gate"
        );
        let evidence: Value = serde_json::from_str(
            unresolved
                .candidates_json
                .as_deref()
                .expect("bridge evidence"),
        )?;
        assert_eq!(
            evidence
                .get("hypotheses")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(2)
        );
        assert!(
            evidence
                .pointer("/hypotheses/0/score")
                .and_then(Value::as_f64)
                .is_some()
        );
        assert!(
            evidence
                .pointer("/hypotheses/1/score")
                .and_then(Value::as_f64)
                .is_some()
        );
        assert!(
            evidence
                .get("runnerUpConfidence")
                .and_then(Value::as_f64)
                .is_some()
        );
        assert!(
            evidence
                .get("winnerMargin")
                .and_then(Value::as_f64)
                .is_some_and(|margin| margin < f64::from(CLASSIFICATION_APPLICATION_MIN_MARGIN))
        );
        assert_eq!(
            evidence
                .pointer("/primaryClassification/hypotheses/0/candidate/ids/tvdbSeries")
                .and_then(Value::as_str),
            Some("305014")
        );

        let clear = selection_for(0.93, 0.80);
        let clear_decision =
            tvdb_anime_bridge_disposition(clear.canonical.as_ref(), clear.winner_margin, 2);
        assert_eq!(clear_decision, ClassificationDisposition::Applied);
        let (hint_json, candidates_json) = build_classification_evidence_payloads(
            &clear.hint,
            &clear.hypotheses,
            clear.runner_up_confidence,
            clear.winner_margin,
        )?;
        let mut unresolved_sibling = seed_outcome();
        unresolved_sibling.disposition = ClassificationDisposition::Unresolved;
        unresolved_sibling.parsed_hint = None;
        unresolved_sibling.accepted_numbers = None;
        let mut outcomes = HashMap::from([
            ("root-a.mkv".to_string(), seed_outcome()),
            ("unresolved-sibling.mkv".to_string(), unresolved_sibling),
        ]);
        apply_tvdb_anime_bridge_outcome(
            &mut outcomes,
            2,
            clear_decision,
            clear.canonical.as_ref().map(|value| value.confidence),
            hint_json,
            candidates_json,
        );
        let applied = outcomes.get("root-a.mkv").expect("bridge outcome");
        assert_eq!(applied.disposition, ClassificationDisposition::Applied);
        assert!(applied.accepted_numbers.is_some());
        assert_eq!(
            outcomes
                .get("unresolved-sibling.mkv")
                .expect("unresolved sibling")
                .disposition,
            ClassificationDisposition::Unresolved
        );

        let mut unscoped_candidate = scored_candidate("20850", 0.99);
        unscoped_candidate.season = None;
        let unscoped = select_best_classification(vec![ClassifiedHint {
            hint: hint.clone(),
            canonical: Some(canonical(0.99, unscoped_candidate)),
        }])
        .expect("unscoped bridge selection");
        assert_eq!(
            tvdb_anime_bridge_disposition(unscoped.canonical.as_ref(), unscoped.winner_margin, 2,),
            ClassificationDisposition::Unresolved,
            "AniList numbering without relation-chain season evidence must not bridge"
        );

        Ok(())
    }

    #[test]
    fn alm4_nfkc_equivalent_titles_share_a_semantic_hypothesis_identity() {
        assert_eq!(
            normalized_title_key("Ｔｏｋｙｏ　Ｇｈｏｕｌ"),
            normalized_title_key("tokyo ghoul")
        );
        assert_eq!(
            dedupe_titles(vec![
                "Ｔｏｋｙｏ　Ｇｈｏｕｌ".to_string(),
                "tokyo ghoul".to_string(),
                "東京喰種".to_string(),
            ]),
            vec!["Ｔｏｋｙｏ　Ｇｈｏｕｌ".to_string(), "東京喰種".to_string(),]
        );
    }

    #[test]
    fn alm4_tvdb_bridge_prerequisite_failure_is_retryable_and_season_scoped() -> Result<()> {
        let hint = alm1_hint(
            ClassifierLibraryType::Series,
            "Tokyo Ghoul Root A",
            Some(2),
            Some(1),
            Some(13),
        );
        let applied_outcome = |season| ClassificationOutcome {
            disposition: ClassificationDisposition::Applied,
            confidence: Some(0.95),
            hint_json: None,
            candidates_json: Some(serde_json::json!({"hypotheses": [{"score": 0.95}]}).to_string()),
            season_scope: Some(season),
            retry_supersedes_applied: false,
            bridge_protected: false,
            parsed_hint: None,
            accepted_numbers: None,
            preserve_authoritative_episode_links: false,
            applied_identity_rows: Default::default(),
        };
        let mut outcomes = HashMap::from([
            ("root-a.mkv".to_string(), applied_outcome(2)),
            ("re.mkv".to_string(), applied_outcome(3)),
        ]);
        let seeds = HashMap::from([(
            2,
            TvdbBridgeSeed {
                hint,
                confidence: 0.95,
                season_number: 2,
            },
        )]);

        mark_tvdb_anime_bridge_prerequisite_unresolved(
            &mut outcomes,
            &seeds,
            "simulated TVDB metadata outage",
        )?;

        let failed = outcomes.get("root-a.mkv").expect("failed bridge season");
        assert_eq!(failed.disposition, ClassificationDisposition::Unresolved);
        assert!(failed.retry_supersedes_applied);
        assert!(
            failed
                .candidates_json
                .as_deref()
                .is_some_and(|evidence| evidence.contains("simulated TVDB metadata outage"))
        );
        assert_eq!(
            outcomes
                .get("re.mkv")
                .expect("unrelated season")
                .disposition,
            ClassificationDisposition::Applied
        );

        Ok(())
    }

    #[test]
    fn alm4_anilist_season_seed_requires_relation_chain_numbering() -> Result<()> {
        let hint = alm1_hint(
            ClassifierLibraryType::Anime,
            "Tokyo Ghoul Root A",
            Some(2),
            Some(1),
            Some(13),
        );
        let mut candidate = alm1_candidate(
            KindHint::Anime,
            "Tokyo Ghoul Root A",
            None,
            Some("20850"),
            None,
            None,
            None,
            1.0,
        );
        candidate.score = 0.96;
        let selection = ClassificationSelection {
            hint: hint.clone(),
            canonical: Some(ClassifierCanonicalMatch {
                kind: KindHint::Anime,
                ids: candidate.ids.clone(),
                season: None,
                episode: None,
                absolute_episode: None,
                confidence: 0.96,
                chosen_provider: "anilist",
                considered: vec![candidate],
            }),
            runner_up_confidence: None,
            winner_margin: None,
            hypotheses: Vec::new(),
        };
        let file = AggregatedFile {
            descriptor: FD {
                path: "root-a.mkv".to_string(),
                size_bytes: None,
                hash: None,
                container: None,
                video_codec: None,
                audio_codec: None,
            },
            source_config_id: None,
            extension_metadata: HashMap::new(),
            season: Some(2),
            episode: Some(1),
            absolute_episode: Some(13),
        };
        let mut updated_ids = ExtIds::default();
        let mut prefer_anime = false;
        let mut tvdb_seeds = HashMap::new();
        let mut anilist_seeds = HashMap::new();

        let outcome = outcome_from_classification_selection(
            Some(selection),
            "root-a.mkv",
            &file,
            MediaType::Anime,
            false,
            false,
            &mut updated_ids,
            &mut prefer_anime,
            &mut tvdb_seeds,
            &mut anilist_seeds,
        )?;

        assert_eq!(outcome.disposition, ClassificationDisposition::Applied);
        assert_eq!(updated_ids.anilist.as_deref(), Some("20850"));
        assert!(anilist_seeds.is_empty());
        assert!(
            outcome.accepted_numbers.is_some(),
            "parser numbering may remain accepted after the identity gate"
        );

        Ok(())
    }

    #[tokio::test]
    async fn alm4_bridge_near_tie_persists_as_automatic_retry_without_episode_mutation()
    -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;

        let series_id = Uuid::new_v4();
        let media_file_id = Uuid::new_v4();
        let media_path = "/media/alm4-bridge-near-tie.mkv";
        sqlx::query("INSERT INTO media_items (id, type, title) VALUES ($1, 'anime', $2)")
            .bind(series_id.to_string())
            .bind("Tokyo Ghoul")
            .execute(&database.pool)
            .await?;
        sqlx::query("INSERT INTO series (id, title, library_type) VALUES ($1, $2, 'anime')")
            .bind(series_id.to_string())
            .bind("Tokyo Ghoul")
            .execute(&database.pool)
            .await?;
        sqlx::query(
            "INSERT INTO media_files (id, media_item_id, path, scan_state) \
             VALUES ($1, $2, $3, 'unresolved')",
        )
        .bind(media_file_id.to_string())
        .bind(series_id.to_string())
        .bind(media_path)
        .execute(&database.pool)
        .await?;

        let bridge_hint = ClassifierHint {
            parser: "tvdb_bridge",
            ..alm1_hint(
                ClassifierLibraryType::Anime,
                "Tokyo Ghoul Root A",
                Some(2),
                Some(1),
                Some(13),
            )
        };
        let hypotheses = vec![
            serde_json::json!({"candidate": {"ids": {"anilist": "20850"}}, "score": 0.92}),
            serde_json::json!({"candidate": {"ids": {"anilist": "22319"}}, "score": 0.90}),
        ];
        let (hint_json, candidates_json) = build_classification_evidence_payloads(
            &bridge_hint,
            &hypotheses,
            Some(0.90),
            Some(0.02),
        )?;
        let mut outcomes = HashMap::from([(
            media_path.to_string(),
            ClassificationOutcome {
                disposition: ClassificationDisposition::Applied,
                confidence: Some(0.92),
                hint_json: None,
                candidates_json: None,
                season_scope: Some(2),
                retry_supersedes_applied: false,
                bridge_protected: false,
                parsed_hint: Some(bridge_hint),
                accepted_numbers: Some(ResolvedEpisodeNumbers {
                    season: Some(2),
                    episode: Some(1),
                    absolute_episode: Some(13),
                }),
                preserve_authoritative_episode_links: false,
                applied_identity_rows: Default::default(),
            },
        )]);
        persist_classification_outcome(
            &database.pool,
            media_file_id,
            outcomes.get(media_path).expect("initial applied outcome"),
        )
        .await?;
        apply_tvdb_anime_bridge_outcome(
            &mut outcomes,
            2,
            ClassificationDisposition::Unresolved,
            Some(0.92),
            hint_json,
            candidates_json,
        );
        let outcome = outcomes.get(media_path).expect("bridge outcome");
        persist_classification_outcome(&database.pool, media_file_id, outcome).await?;

        let persisted = sqlx::query(
            "SELECT disposition, candidates_json FROM classifier_resolution_state \
             WHERE media_file_id = $1",
        )
        .bind(media_file_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(persisted.try_get::<String, _>("disposition")?, "unresolved");
        let evidence: Value =
            serde_json::from_str(&persisted.try_get::<String, _>("candidates_json")?)?;
        assert_eq!(
            evidence
                .get("hypotheses")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(2)
        );
        assert!(
            evidence
                .get("runnerUpConfidence")
                .and_then(Value::as_f64)
                .is_some_and(|value| (value - 0.9).abs() < 0.000_001)
        );
        assert!(
            evidence
                .get("winnerMargin")
                .and_then(Value::as_f64)
                .is_some_and(|value| (value - 0.02).abs() < 0.000_001)
        );
        assert!(
            load_existing_classification_for_path(
                &database.pool,
                media_path,
                MediaType::Anime,
                false,
            )
            .await?
            .is_none(),
            "unresolved bridge state must be classified again on the next scan"
        );
        let episode_link_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM episode_files WHERE media_file_id = $1")
                .bind(media_file_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        let review_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM review_queue")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(episode_link_count, 0);
        assert_eq!(review_count, 0);

        Ok(())
    }

    #[tokio::test]
    async fn alm1_unresolved_candidate_is_inert_then_retries_and_links_idempotently() -> Result<()>
    {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let media_dir = tempdir()?;
        let media_path = media_dir.path().join("Tokyo Ghoul Root A S02E01.mkv");
        std::fs::write(&media_path, b"dummy")?;
        let media_path = media_path.to_string_lossy().to_string();

        let candidates = Arc::new(RwLock::new(vec![alm1_candidate(
            KindHint::Anime,
            "Completely Different Anime",
            Some("wrong-tvdb"),
            Some("wrong-anilist"),
            Some(2),
            Some(1),
            Some(13),
            0.1,
        )]));
        let calls = Arc::new(AtomicUsize::new(0));
        let pipeline = alm1_classifier_pipeline(
            alm1_hint(
                ClassifierLibraryType::Series,
                "Tokyo Ghoul Root A",
                Some(2),
                Some(1),
                Some(13),
            ),
            candidates.clone(),
            calls.clone(),
        );
        let scan_candidate = || MediaFileCandidate {
            identity: MediaIdentity {
                r#type: MediaType::Series,
                external_ids: ExtIds::default(),
                title: "Tokyo Ghoul Root A".to_string(),
                year: Some(2024),
                season: Some(2),
                episode: Some(1),
            },
            files: vec![FD {
                path: media_path.clone(),
                size_bytes: Some(2048),
                hash: None,
                container: Some("mkv".to_string()),
                video_codec: Some("h264".to_string()),
                audio_codec: Some("aac".to_string()),
            }],
            extension_metadata: HashMap::new(),
            source_config_id: None,
        };

        run_full_scan_with_classifier(
            &database.pool,
            None,
            None,
            None,
            None,
            &pipeline,
            vec![scan_candidate()],
            false,
            false,
            false,
        )
        .await?;

        let unresolved = sqlx::query(
            "SELECT s.library_type, s.external_tvdb_series, s.external_anilist, \
                    mf.id AS media_file_id, mf.media_item_id, rs.disposition, rs.candidates_json \
             FROM series s \
             JOIN media_files mf ON mf.media_item_id = s.id \
             JOIN classifier_resolution_state rs ON rs.media_file_id = mf.id \
             WHERE mf.path = $1",
        )
        .bind(&media_path)
        .fetch_one(&database.pool)
        .await?;
        let media_file_id: String = unresolved.try_get("media_file_id")?;
        assert_eq!(unresolved.try_get::<String, _>("library_type")?, "series");
        assert_eq!(
            unresolved.try_get::<Option<String>, _>("external_tvdb_series")?,
            None
        );
        assert_eq!(
            unresolved.try_get::<Option<String>, _>("external_anilist")?,
            None
        );
        assert_eq!(
            unresolved.try_get::<String, _>("disposition")?,
            "unresolved"
        );
        let evidence: String = unresolved.try_get("candidates_json")?;
        let evidence: Value = serde_json::from_str(&evidence)?;
        assert_eq!(
            evidence
                .pointer("/hypotheses/0/candidate/ids/tvdbSeries")
                .and_then(Value::as_str),
            Some("wrong-tvdb")
        );
        assert!(
            evidence
                .pointer("/hypotheses/0/score")
                .and_then(Value::as_f64)
                .is_some()
        );
        for table in [
            "series_external_ids",
            "season_external_ids",
            "seasons",
            "episodes",
            "episode_files",
        ] {
            let query = format!("SELECT COUNT(*) FROM {table}");
            let count: i64 = sqlx::query_scalar(&query).fetch_one(&database.pool).await?;
            assert_eq!(count, 0, "{table} must not contain provisional mutation");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        *candidates.write().expect("ALM-1 candidate lock poisoned") = vec![alm1_candidate(
            KindHint::Series,
            "Tokyo Ghoul Root A",
            Some("correct-tvdb"),
            None,
            Some(2),
            Some(1),
            Some(13),
            1.0,
        )];

        run_full_scan_with_classifier(
            &database.pool,
            None,
            None,
            None,
            None,
            &pipeline,
            vec![scan_candidate()],
            false,
            false,
            false,
        )
        .await?;

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let resolved = sqlx::query(
            "SELECT s.external_tvdb_series, e.season_number, e.episode_number, \
                    e.absolute_episode_number, CAST(e.has_file AS INTEGER) AS has_file, \
                    mf.id AS media_file_id, rs.disposition \
             FROM series s \
             JOIN episodes e ON e.series_id = s.id \
             JOIN episode_files ef ON ef.episode_id = e.id \
             JOIN media_files mf ON mf.id = ef.media_file_id \
             JOIN classifier_resolution_state rs ON rs.media_file_id = mf.id \
             WHERE mf.path = $1",
        )
        .bind(&media_path)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            resolved.try_get::<String, _>("external_tvdb_series")?,
            "correct-tvdb"
        );
        assert_eq!(resolved.try_get::<i64, _>("season_number")?, 2);
        assert_eq!(resolved.try_get::<i64, _>("episode_number")?, 1);
        assert_eq!(resolved.try_get::<i64, _>("absolute_episode_number")?, 13);
        assert_eq!(resolved.try_get::<i64, _>("has_file")?, 1);
        assert_eq!(
            resolved.try_get::<String, _>("media_file_id")?,
            media_file_id
        );
        assert_eq!(resolved.try_get::<String, _>("disposition")?, "applied");

        run_full_scan_with_classifier(
            &database.pool,
            None,
            None,
            None,
            None,
            &pipeline,
            vec![scan_candidate()],
            false,
            false,
            false,
        )
        .await?;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "an applied, linked file should use persisted classification"
        );
        for (table, expected) in [
            ("series", 1_i64),
            ("media_files", 1),
            ("classifier_resolution_state", 1),
            ("seasons", 1),
            ("episodes", 1),
            ("episode_files", 1),
            ("series_external_ids", 1),
        ] {
            let query = format!("SELECT COUNT(*) FROM {table}");
            let count: i64 = sqlx::query_scalar(&query).fetch_one(&database.pool).await?;
            assert_eq!(count, expected, "{table} must remain idempotent");
        }

        *candidates.write().expect("ALM-1 candidate lock poisoned") = vec![alm1_candidate(
            KindHint::Anime,
            "Forced Bad Result",
            Some("forced-wrong-tvdb"),
            Some("forced-wrong-anilist"),
            Some(1),
            Some(99),
            Some(99),
            0.01,
        )];
        run_full_scan_with_classifier(
            &database.pool,
            None,
            None,
            None,
            None,
            &pipeline,
            vec![scan_candidate()],
            false,
            true,
            false,
        )
        .await?;
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        let preserved: (String, i64, i64, String) = sqlx::query_as(
            "SELECT s.external_tvdb_series, e.season_number, e.episode_number, rs.disposition \
             FROM series s \
             JOIN episodes e ON e.series_id = s.id \
             JOIN episode_files ef ON ef.episode_id = e.id \
             JOIN classifier_resolution_state rs ON rs.media_file_id = ef.media_file_id",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            preserved,
            ("correct-tvdb".to_string(), 2, 1, "applied".to_string())
        );

        Ok(())
    }

    #[tokio::test]
    async fn alm1_absolute_only_unresolved_file_never_creates_s01e01() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let media_dir = tempdir()?;
        let media_path = media_dir.path().join("Tokyo Ghoul Root A - 13.mkv");
        std::fs::write(&media_path, b"dummy")?;
        let media_path = media_path.to_string_lossy().to_string();
        let candidates = Arc::new(RwLock::new(Vec::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let pipeline = alm1_classifier_pipeline(
            alm1_hint(
                ClassifierLibraryType::Anime,
                "Tokyo Ghoul Root A",
                None,
                None,
                Some(13),
            ),
            candidates,
            calls,
        );

        run_full_scan_with_classifier(
            &database.pool,
            None,
            None,
            None,
            None,
            &pipeline,
            vec![MediaFileCandidate {
                identity: MediaIdentity {
                    r#type: MediaType::Anime,
                    external_ids: ExtIds::default(),
                    title: "Tokyo Ghoul Root A".to_string(),
                    year: Some(2015),
                    season: None,
                    episode: None,
                },
                files: vec![FD {
                    path: media_path.clone(),
                    size_bytes: Some(2048),
                    hash: None,
                    container: Some("mkv".to_string()),
                    video_codec: Some("h264".to_string()),
                    audio_codec: Some("aac".to_string()),
                }],
                extension_metadata: HashMap::new(),
                source_config_id: None,
            }],
            false,
            false,
            false,
        )
        .await?;

        let row = sqlx::query(
            "SELECT mf.media_item_id, s.id AS series_id, rs.disposition, rs.hint_json \
             FROM media_files mf \
             JOIN series s ON s.id = mf.media_item_id \
             JOIN classifier_resolution_state rs ON rs.media_file_id = mf.id \
             WHERE mf.path = $1",
        )
        .bind(&media_path)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            row.try_get::<String, _>("media_item_id")?,
            row.try_get::<String, _>("series_id")?
        );
        assert_eq!(row.try_get::<String, _>("disposition")?, "unresolved");
        let hint: String = row.try_get("hint_json")?;
        let hint: Value = serde_json::from_str(&hint)?;
        assert_eq!(
            hint.get("absoluteEpisode").and_then(Value::as_i64),
            Some(13)
        );
        for table in ["seasons", "episodes", "episode_files"] {
            let query = format!("SELECT COUNT(*) FROM {table}");
            let count: i64 = sqlx::query_scalar(&query).fetch_one(&database.pool).await?;
            assert_eq!(count, 0, "{table} must not contain a synthetic S01E01");
        }

        Ok(())
    }

    #[tokio::test]
    async fn alm1_production_parsers_keep_episode_only_anime_numbering_unresolved() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let media_dir = tempdir()?;
        let media_path = media_dir.path().join("Tokyo Ghoul Root A - 13.mkv");
        std::fs::write(&media_path, b"dummy")?;
        let media_path = media_path.to_string_lossy().to_string();
        let candidates = Arc::new(RwLock::new(vec![alm1_candidate(
            KindHint::Anime,
            "Tokyo Ghoul Root A",
            Some("305014"),
            None,
            None,
            None,
            None,
            1.0,
        )]));
        let calls = Arc::new(AtomicUsize::new(0));
        let pipeline = alm1_production_parser_pipeline(candidates, calls.clone());

        run_full_scan_with_classifier(
            &database.pool,
            None,
            None,
            None,
            None,
            &pipeline,
            vec![alm1_scan_candidate(
                &media_path,
                MediaType::Anime,
                "Tokyo Ghoul Root A",
                Some(2015),
                None,
                None,
            )],
            false,
            false,
            false,
        )
        .await?;

        assert!(calls.load(Ordering::SeqCst) >= 2);
        let row = sqlx::query(
            "SELECT rs.disposition, rs.candidates_json \
             FROM classifier_resolution_state rs \
             JOIN media_files mf ON mf.id = rs.media_file_id \
             WHERE mf.path = $1",
        )
        .bind(&media_path)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(row.try_get::<String, _>("disposition")?, "unresolved");
        let evidence: Value = serde_json::from_str(&row.try_get::<String, _>("candidates_json")?)?;
        let hypotheses = evidence
            .get("hypotheses")
            .and_then(Value::as_array)
            .expect("production parser hypotheses");
        assert!(!hypotheses.is_empty());
        assert!(
            hypotheses
                .iter()
                .all(|entry| entry.pointer("/hint/season").is_none_or(Value::is_null)),
            "episode-only production hints must not manufacture season one: {hypotheses:?}"
        );
        assert!(hypotheses.iter().any(|entry| {
            entry
                .pointer("/hint/absoluteEpisode")
                .and_then(Value::as_i64)
                == Some(13)
        }));
        let episode_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM episodes")
            .fetch_one(&database.pool)
            .await?;
        let manual_review_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM review_queue")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(episode_count, 0);
        assert_eq!(manual_review_count, 0);

        Ok(())
    }

    #[tokio::test]
    async fn alm1_provider_failure_is_stored_and_retried_without_aborting_scan() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let media_dir = tempdir()?;
        let media_path = media_dir.path().join("Outage Anime - 13.mkv");
        std::fs::write(&media_path, b"dummy")?;
        let media_path = media_path.to_string_lossy().to_string();
        let calls = Arc::new(AtomicUsize::new(0));
        let pipeline = ClassifierPipeline::new()
            .register_hint_parser(Arc::new(Alm1FixedHintParser {
                hint: alm1_hint(
                    ClassifierLibraryType::Anime,
                    "Outage Anime",
                    None,
                    None,
                    Some(13),
                ),
            }))
            .register_identifier_provider(Arc::new(Alm1FailingIdentifier {
                calls: calls.clone(),
            }));
        let scan_candidate = || {
            alm1_scan_candidate(
                &media_path,
                MediaType::Anime,
                "Outage Anime",
                Some(2024),
                None,
                None,
            )
        };

        for expected_calls in 1..=2 {
            run_full_scan_with_classifier(
                &database.pool,
                None,
                None,
                None,
                None,
                &pipeline,
                vec![scan_candidate()],
                false,
                false,
                false,
            )
            .await?;
            assert_eq!(calls.load(Ordering::SeqCst), expected_calls);
        }

        let row = sqlx::query(
            "SELECT rs.disposition, rs.candidates_json \
             FROM classifier_resolution_state rs \
             JOIN media_files mf ON mf.id = rs.media_file_id \
             WHERE mf.path = $1",
        )
        .bind(&media_path)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(row.try_get::<String, _>("disposition")?, "unresolved");
        let evidence: Value = serde_json::from_str(&row.try_get::<String, _>("candidates_json")?)?;
        assert_eq!(
            evidence.get("classificationError").and_then(Value::as_str),
            Some("simulated classifier provider outage")
        );
        for table in ["media_files", "classifier_resolution_state"] {
            let query = format!("SELECT COUNT(*) FROM {table}");
            let count: i64 = sqlx::query_scalar(&query).fetch_one(&database.pool).await?;
            assert_eq!(count, 1, "{table} retry state must be idempotent");
        }
        let link_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM episode_files")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(link_count, 0);

        Ok(())
    }

    #[tokio::test]
    async fn alm1_low_confidence_new_file_cannot_mutate_existing_series_identity() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let series_id = Uuid::new_v4();
        let canonical_ids = ExtIds {
            imdb: Some("tt9990001".to_string()),
            tvdb_series: Some("305014".to_string()),
            anilist: Some("100240".to_string()),
            ..Default::default()
        };
        sqlx::query(
            "INSERT INTO media_items (id, type, external_ids, title, year) \
             VALUES ($1, 'anime', $2, 'Existing Canonical Anime', 2015)",
        )
        .bind(series_id.to_string())
        .bind(serde_json::to_string(&canonical_ids)?)
        .execute(&database.pool)
        .await?;
        sqlx::query(
            "INSERT INTO series (id, title, year, library_type, external_imdb, \
             external_tvdb_series, external_anilist) \
             VALUES ($1, 'Existing Canonical Anime', 2015, 'anime', \
                     'tt9990001', '305014', '100240')",
        )
        .bind(series_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query(
            "INSERT INTO series_external_ids \
             (id, series_id, provider, external_id, source) \
             VALUES ($1, $2, 'tvdb', '305014', 'fixture')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(series_id.to_string())
        .execute(&database.pool)
        .await?;

        let media_dir = tempdir()?;
        let media_path = media_dir.path().join("Existing Canonical Anime S02E01.mkv");
        std::fs::write(&media_path, b"dummy")?;
        let media_path = media_path.to_string_lossy().to_string();
        let pipeline = alm1_classifier_pipeline(
            alm1_hint(
                ClassifierLibraryType::Anime,
                "Existing Canonical Anime",
                Some(2),
                Some(1),
                Some(13),
            ),
            Arc::new(RwLock::new(vec![alm1_candidate(
                KindHint::Anime,
                "Wrong Result",
                Some("wrong-tvdb"),
                Some("wrong-anilist"),
                Some(2),
                Some(1),
                Some(13),
                0.05,
            )])),
            Arc::new(AtomicUsize::new(0)),
        );

        run_full_scan_with_classifier(
            &database.pool,
            None,
            None,
            None,
            None,
            &pipeline,
            vec![alm1_scan_candidate(
                &media_path,
                MediaType::Series,
                "Existing Canonical Anime",
                Some(2015),
                Some(2),
                Some(1),
            )],
            false,
            false,
            false,
        )
        .await?;

        let identity = sqlx::query(
            "SELECT title, year, library_type, external_imdb, external_tvdb_series, \
                    external_anilist FROM series WHERE id = $1",
        )
        .bind(series_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            identity.try_get::<String, _>("title")?,
            "Existing Canonical Anime"
        );
        assert_eq!(identity.try_get::<i64, _>("year")?, 2015);
        assert_eq!(identity.try_get::<String, _>("library_type")?, "anime");
        assert_eq!(identity.try_get::<String, _>("external_imdb")?, "tt9990001");
        assert_eq!(
            identity.try_get::<String, _>("external_tvdb_series")?,
            "305014"
        );
        assert_eq!(identity.try_get::<String, _>("external_anilist")?, "100240");
        let legacy_ids: String =
            sqlx::query_scalar("SELECT external_ids FROM media_items WHERE id = $1")
                .bind(series_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(serde_json::from_str::<ExtIds>(&legacy_ids)?, canonical_ids);
        let external_id_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM series_external_ids WHERE series_id = $1")
                .bind(series_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(external_id_count, 1);
        let media_item_id: String =
            sqlx::query_scalar("SELECT media_item_id FROM media_files WHERE path = $1")
                .bind(&media_path)
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(media_item_id, series_id.to_string());
        let mutation_count: i64 = sqlx::query_scalar(
            "SELECT (SELECT COUNT(*) FROM seasons) + \
                    (SELECT COUNT(*) FROM episodes) + \
                    (SELECT COUNT(*) FROM episode_files)",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(mutation_count, 0);

        Ok(())
    }

    #[tokio::test]
    async fn alm1_applied_sibling_does_not_starve_unresolved_file_retry() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let media_dir = tempdir()?;
        let applied_path = media_dir.path().join("Mixed Anime S01E01.mkv");
        let unresolved_path = media_dir.path().join("Mixed Anime - 13.mkv");
        std::fs::write(&applied_path, b"applied")?;
        std::fs::write(&unresolved_path, b"unresolved")?;
        let applied_path = applied_path.to_string_lossy().to_string();
        let unresolved_path = unresolved_path.to_string_lossy().to_string();
        let mut hints = HashMap::new();
        hints.insert(
            applied_path.clone(),
            alm1_hint(
                ClassifierLibraryType::Anime,
                "Mixed Anime",
                Some(1),
                Some(1),
                Some(1),
            ),
        );
        hints.insert(
            unresolved_path.clone(),
            alm1_hint(
                ClassifierLibraryType::Anime,
                "Unrelated Noise",
                None,
                None,
                Some(13),
            ),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let pipeline = ClassifierPipeline::new()
            .register_hint_parser(Arc::new(Alm1PathHintParser { hints }))
            .register_identifier_provider(Arc::new(Alm1MutableIdentifier {
                candidates: Arc::new(RwLock::new(vec![alm1_candidate(
                    KindHint::Anime,
                    "Mixed Anime",
                    Some("mixed-tvdb"),
                    None,
                    None,
                    None,
                    None,
                    1.0,
                )])),
                calls: calls.clone(),
            }));
        let scan_candidates = || {
            vec![
                alm1_scan_candidate(
                    &applied_path,
                    MediaType::Anime,
                    "Mixed Anime",
                    Some(2024),
                    Some(1),
                    Some(1),
                ),
                alm1_scan_candidate(
                    &unresolved_path,
                    MediaType::Anime,
                    "Mixed Anime",
                    Some(2024),
                    None,
                    None,
                ),
            ]
        };

        run_full_scan_with_classifier(
            &database.pool,
            None,
            None,
            None,
            None,
            &pipeline,
            scan_candidates(),
            false,
            false,
            false,
        )
        .await?;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let states: Vec<(String, String)> = sqlx::query_as(
            "SELECT mf.path, rs.disposition \
             FROM media_files mf \
             JOIN classifier_resolution_state rs ON rs.media_file_id = mf.id \
             ORDER BY mf.path",
        )
        .fetch_all(&database.pool)
        .await?;
        assert_eq!(states.len(), 2);
        assert!(states.contains(&(applied_path.clone(), "applied".to_string())));
        assert!(states.contains(&(unresolved_path.clone(), "unresolved".to_string())));
        let links: Vec<String> = sqlx::query_scalar(
            "SELECT mf.path FROM media_files mf \
             JOIN episode_files ef ON ef.media_file_id = mf.id",
        )
        .fetch_all(&database.pool)
        .await?;
        assert_eq!(links, vec![applied_path.clone()]);

        run_full_scan_with_classifier(
            &database.pool,
            None,
            None,
            None,
            None,
            &pipeline,
            scan_candidates(),
            false,
            false,
            false,
        )
        .await?;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "only the unresolved sibling should retry"
        );

        Ok(())
    }

    #[test]
    fn alm1_applied_canonical_numbers_override_noisy_scan_numbers() {
        let file = AggregatedFile {
            descriptor: FD {
                path: "/media/noisy-scan.mkv".to_string(),
                size_bytes: None,
                hash: None,
                container: None,
                video_codec: None,
                audio_codec: None,
            },
            source_config_id: None,
            extension_metadata: HashMap::new(),
            season: Some(1),
            episode: Some(1),
            absolute_episode: None,
        };
        let outcome = ClassificationOutcome {
            disposition: ClassificationDisposition::Applied,
            confidence: Some(0.99),
            hint_json: None,
            candidates_json: None,
            season_scope: Some(2),
            retry_supersedes_applied: false,
            bridge_protected: false,
            parsed_hint: None,
            accepted_numbers: Some(ResolvedEpisodeNumbers {
                season: Some(2),
                episode: Some(3),
                absolute_episode: Some(15),
            }),
            preserve_authoritative_episode_links: false,
            applied_identity_rows: Default::default(),
        };

        let resolved =
            resolve_episode_numbers(&file, Some(&outcome), MediaType::Anime, &HashMap::new());
        assert_eq!(resolved.season, Some(2));
        assert_eq!(resolved.episode, Some(3));
        assert_eq!(resolved.absolute_episode, Some(15));
    }

    #[tokio::test]
    async fn alm1_retry_merges_into_canonical_series_and_removes_placeholder() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let canonical_series_id = Uuid::new_v4();
        let canonical_ids = ExtIds {
            tvdb_series: Some("305014".to_string()),
            ..Default::default()
        };
        sqlx::query(
            "INSERT INTO media_items (id, type, external_ids, title, year) \
             VALUES ($1, 'anime', $2, 'Tokyo Ghoul', 2014)",
        )
        .bind(canonical_series_id.to_string())
        .bind(serde_json::to_string(&canonical_ids)?)
        .execute(&database.pool)
        .await?;
        sqlx::query(
            "INSERT INTO series \
             (id, title, year, library_type, external_tvdb_series) \
             VALUES ($1, 'Tokyo Ghoul', 2014, 'anime', '305014')",
        )
        .bind(canonical_series_id.to_string())
        .execute(&database.pool)
        .await?;

        let media_dir = tempdir()?;
        let media_path = media_dir.path().join("Tokyo Ghoul Root A S02E01.mkv");
        std::fs::write(&media_path, b"dummy")?;
        let media_path = media_path.to_string_lossy().to_string();
        let candidates = Arc::new(RwLock::new(vec![alm1_candidate(
            KindHint::Anime,
            "Wrong Anime",
            Some("wrong-tvdb"),
            None,
            Some(2),
            Some(1),
            Some(13),
            0.05,
        )]));
        let calls = Arc::new(AtomicUsize::new(0));
        let pipeline = alm1_classifier_pipeline(
            alm1_hint(
                ClassifierLibraryType::Anime,
                "Tokyo Ghoul Root A",
                Some(2),
                Some(1),
                Some(13),
            ),
            candidates.clone(),
            calls.clone(),
        );
        let scan_candidate = || {
            alm1_scan_candidate(
                &media_path,
                MediaType::Anime,
                "Tokyo Ghoul Root A",
                Some(2015),
                Some(2),
                Some(1),
            )
        };

        run_full_scan_with_classifier(
            &database.pool,
            None,
            None,
            None,
            None,
            &pipeline,
            vec![scan_candidate()],
            false,
            false,
            false,
        )
        .await?;
        let placeholder_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM series")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(placeholder_count, 2);

        *candidates.write().expect("ALM-1 candidate lock poisoned") = vec![alm1_candidate(
            KindHint::Anime,
            "Tokyo Ghoul Root A",
            Some("305014"),
            None,
            Some(2),
            Some(1),
            Some(13),
            1.0,
        )];
        run_full_scan_with_classifier(
            &database.pool,
            None,
            None,
            None,
            None,
            &pipeline,
            vec![scan_candidate()],
            false,
            false,
            false,
        )
        .await?;

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let series_rows = sqlx::query("SELECT id, title FROM series ORDER BY id")
            .fetch_all(&database.pool)
            .await?;
        assert_eq!(series_rows.len(), 1);
        assert_eq!(
            series_rows[0].try_get::<String, _>("id")?,
            canonical_series_id.to_string()
        );
        assert_eq!(series_rows[0].try_get::<String, _>("title")?, "Tokyo Ghoul");
        let legacy_title: String =
            sqlx::query_scalar("SELECT title FROM media_items WHERE id = $1")
                .bind(canonical_series_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(legacy_title, "Tokyo Ghoul");
        let linkage = sqlx::query(
            "SELECT mf.media_item_id, e.series_id, e.season_number, e.episode_number \
             FROM media_files mf \
             JOIN episode_files ef ON ef.media_file_id = mf.id \
             JOIN episodes e ON e.id = ef.episode_id \
             WHERE mf.path = $1",
        )
        .bind(&media_path)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            linkage.try_get::<String, _>("media_item_id")?,
            canonical_series_id.to_string()
        );
        assert_eq!(
            linkage.try_get::<String, _>("series_id")?,
            canonical_series_id.to_string()
        );
        assert_eq!(linkage.try_get::<i64, _>("season_number")?, 2);
        assert_eq!(linkage.try_get::<i64, _>("episode_number")?, 1);

        Ok(())
    }

    #[tokio::test]
    async fn managed_ingest_intent_supplies_ids_and_marks_match() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;

        let title = "Managed Movie";
        let normalized_title = normalize_managed_intent_title(title);
        let provider_id = Uuid::new_v4();
        install_test_managed_provider(
            &database.pool,
            provider_id,
            "radarr",
            "media.manager.movies",
        )
        .await?;
        let external_ids_json = serde_json::json!({
            "imdb": "tt0096256"
        });

        sqlx::query(
            "INSERT INTO managed_ingest_intents (
                intent_id, media_type, title, normalized_title, year, external_ids_json,
                manager_provider_id, manager_item_id, manager_label, source, active
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind("movie")
        .bind(title)
        .bind(normalized_title)
        .bind(1988)
        .bind(serde_json::to_string(&external_ids_json)?)
        .bind(provider_id.to_string())
        .bind("movie-123")
        .bind("default(radarr)")
        .bind("find_media_add")
        .execute(&database.pool)
        .await?;

        let candidates = vec![MediaFileCandidate {
            identity: MediaIdentity {
                r#type: MediaType::Movie,
                external_ids: ExtIds::default(),
                title: title.to_string(),
                year: Some(1988),
                season: None,
                episode: None,
            },
            files: vec![FD {
                path: "/media/managed_movie.mkv".to_string(),
                size_bytes: Some(2048),
                hash: None,
                container: Some("mkv".to_string()),
                video_codec: Some("h264".to_string()),
                audio_codec: Some("aac".to_string()),
            }],
            extension_metadata: HashMap::new(),
            source_config_id: None,
        }];
        run_full_scan(&database.pool, candidates, false).await?;

        let imdb: Option<String> = sqlx::query_scalar("SELECT external_imdb FROM movies LIMIT 1")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(imdb.as_deref(), Some("tt0096256"));

        let (pending_count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM review_queue WHERE status = 'pending'")
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(pending_count, 0);

        let matched: Option<String> = sqlx::query_scalar(
            "SELECT CAST(last_matched_at AS TEXT) FROM managed_ingest_intents LIMIT 1",
        )
        .fetch_one(&database.pool)
        .await?;
        assert!(
            matched
                .as_deref()
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false)
        );

        Ok(())
    }

    #[tokio::test]
    async fn managed_ingest_intent_persists_library_provenance() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let store = ExtensionStore::new(&database.pool);
        let instance_id = Uuid::new_v4();
        let provider_id = Uuid::new_v4();

        store
            .upsert_extension(&NewExtension {
                extension_id: "elixir.modules.radarr".to_string(),
                name: "Radarr".to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Community,
                manifest_json: serde_json::json!({}),
                package_hash: None,
                enabled: true,
            })
            .await?;
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.radarr".to_string(),
                instance_name: "default".to_string(),
                config_json: Some(serde_json::json!({})),
                enabled: true,
            })
            .await?;
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: "media.manager.movies".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("radarr".to_string()),
                scope_json: None,
                endpoint_json: None,
                health_state: ProviderHealthState::Healthy,
            })
            .await?;
        store
            .upsert_managed_ingest_intent(&NewManagedIngestIntent {
                media_type: MediaType::Movie,
                title: "Managed Movie".to_string(),
                normalized_title: normalize_managed_intent_title("Managed Movie"),
                year: Some(1988),
                external_ids: Some(ExtIds {
                    imdb: Some("tt0096256".to_string()),
                    ..Default::default()
                }),
                manager_provider_id: provider_id,
                manager_item_id: Some("movie-123".to_string()),
                manager_label: Some("default (radarr)".to_string()),
                source: "find_media_add".to_string(),
            })
            .await?;

        let candidates = vec![MediaFileCandidate {
            identity: MediaIdentity {
                r#type: MediaType::Movie,
                external_ids: ExtIds::default(),
                title: "Managed Movie".to_string(),
                year: Some(1988),
                season: None,
                episode: None,
            },
            files: vec![FD {
                path: "/media/managed_provenance_movie.mkv".to_string(),
                size_bytes: Some(2048),
                hash: None,
                container: Some("mkv".to_string()),
                video_codec: Some("h264".to_string()),
                audio_codec: Some("aac".to_string()),
            }],
            extension_metadata: HashMap::new(),
            source_config_id: None,
        }];
        run_full_scan(&database.pool, candidates, false).await?;

        let row = sqlx::query(
            "SELECT manager_provider_id, manager_item_id, manager_implementation
             FROM managed_library_provenance
             LIMIT 1",
        )
        .fetch_one(&database.pool)
        .await?;
        let stored_provider_id: String = row.try_get("manager_provider_id")?;
        let stored_manager_item_id: Option<String> = row.try_get("manager_item_id").ok();
        let stored_implementation: Option<String> = row.try_get("manager_implementation").ok();
        assert_eq!(stored_provider_id, provider_id.to_string());
        assert_eq!(stored_manager_item_id.as_deref(), Some("movie-123"));
        assert_eq!(stored_implementation.as_deref(), Some("radarr"));

        let owner = sqlx::query(
            "SELECT owner_type, owner_label, owner_provider_id, owner_external_id, release_capability, release_policy
             FROM media_ownerships
             LIMIT 1",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(owner.get::<String, _>("owner_type"), "extension");
        let owner_label: Option<String> = owner.try_get("owner_label").ok();
        assert_eq!(owner_label.as_deref(), Some("default (radarr)"));
        let expected_provider_id = provider_id.to_string();
        let owner_provider_id: Option<String> = owner.try_get("owner_provider_id").ok();
        assert_eq!(
            owner_provider_id.as_deref(),
            Some(expected_provider_id.as_str())
        );
        let owner_external_id: Option<String> = owner.try_get("owner_external_id").ok();
        assert_eq!(owner_external_id.as_deref(), Some("movie-123"));
        assert_eq!(
            owner.get::<String, _>("release_capability"),
            "manager.remove_item"
        );
        assert_eq!(owner.get::<String, _>("release_policy"), "supported");

        Ok(())
    }

    #[tokio::test]
    async fn managed_movie_import_uses_intent_identity_and_resists_scan_override() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let provider_id = Uuid::new_v4();
        install_test_managed_provider(
            &database.pool,
            provider_id,
            "radarr",
            "media.manager.movies",
        )
        .await?;
        let file_dir = tempdir()?;
        let file_path = file_dir
            .path()
            .join("Casino Royale 2006 BluRay 1080p DDP 5 1 x264-hallowed.mkv");
        std::fs::write(&file_path, b"dummy")?;

        let external_ids_json = serde_json::json!({
            "imdb": "tt0381061",
            "tmdb": "36557"
        });
        sqlx::query(
            "INSERT INTO managed_ingest_intents (
                intent_id, media_type, title, normalized_title, year, external_ids_json,
                manager_provider_id, manager_item_id, manager_label, source, active
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind("movie")
        .bind("Casino Royale")
        .bind(normalize_managed_intent_title("Casino Royale"))
        .bind(2006)
        .bind(serde_json::to_string(&external_ids_json)?)
        .bind(provider_id.to_string())
        .bind("1")
        .bind("default (radarr)")
        .bind("find_media_add")
        .execute(&database.pool)
        .await?;

        let store = ExtensionStore::new(&database.pool);
        let intent = store
            .list_active_managed_ingest_intents()
            .await?
            .into_iter()
            .next()
            .expect("managed intent");
        ingest_managed_movie_import(
            &database.pool,
            None,
            None,
            &intent,
            &file_path.to_string_lossy(),
        )
        .await?;

        let row =
            sqlx::query("SELECT title, year, external_imdb, external_tmdb FROM movies LIMIT 1")
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(row.get::<String, _>("title"), "Casino Royale");
        assert_eq!(row.get::<i32, _>("year"), 2006);
        assert_eq!(
            row.try_get::<String, _>("external_imdb").ok().as_deref(),
            Some("tt0381061")
        );
        assert_eq!(
            row.try_get::<String, _>("external_tmdb").ok().as_deref(),
            Some("36557")
        );
        let matched: Option<String> = sqlx::query_scalar(
            "SELECT CAST(last_matched_at AS TEXT) FROM managed_ingest_intents LIMIT 1",
        )
        .fetch_one(&database.pool)
        .await?;
        assert!(matched.as_deref().is_some_and(|value| !value.is_empty()));
        let (provenance_count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM managed_library_provenance")
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(provenance_count, 1);

        let noisy_scan = vec![MediaFileCandidate {
            identity: MediaIdentity {
                r#type: MediaType::Movie,
                external_ids: ExtIds::default(),
                title: "Casino Royale DDP 5 1 hallowed".to_string(),
                year: None,
                season: None,
                episode: None,
            },
            files: vec![FD {
                path: file_path.to_string_lossy().to_string(),
                size_bytes: Some(2048),
                hash: None,
                container: Some("mkv".to_string()),
                video_codec: Some("h264".to_string()),
                audio_codec: Some("aac".to_string()),
            }],
            extension_metadata: HashMap::new(),
            source_config_id: None,
        }];
        run_full_scan(&database.pool, noisy_scan, true).await?;

        let (movie_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM movies")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(movie_count, 1);
        let row = sqlx::query("SELECT title, year FROM movies LIMIT 1")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(row.get::<String, _>("title"), "Casino Royale");
        assert_eq!(row.get::<i32, _>("year"), 2006);

        Ok(())
    }

    #[tokio::test]
    async fn managed_import_event_hydrates_movie_metadata_and_artwork() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let store = ExtensionStore::new(&database.pool);
        let provider_id = Uuid::new_v4();
        install_test_managed_provider(
            &database.pool,
            provider_id,
            "radarr",
            "media.manager.movies",
        )
        .await?;
        let file_dir = tempdir()?;
        let file_path = file_dir
            .path()
            .join("Casino Royale 2006 BluRay 1080p DDP 5 1 x264-hallowed.mkv");
        std::fs::write(&file_path, b"dummy")?;
        let artwork_dir = tempdir()?;
        let (cinemeta_base_url, shutdown_tx) = start_mock_cinemeta_server().await?;

        let mut metadata_config = MetadataConfig::default();
        metadata_config.cinemeta_base_url = cinemeta_base_url;
        metadata_config.request_timeout_seconds = 2;
        let metadata = MetadataService::new(metadata_config)?;
        let artwork = ArtworkService::new(artwork_dir.path(), 2)?;

        let intent_id = store
            .upsert_managed_ingest_intent(&NewManagedIngestIntent {
                media_type: MediaType::Movie,
                title: "Casino Royale".to_string(),
                normalized_title: normalize_managed_intent_title("Casino Royale"),
                year: Some(2006),
                external_ids: Some(ExtIds {
                    imdb: Some("tt0381061".to_string()),
                    tmdb: Some("36557".to_string()),
                    ..Default::default()
                }),
                manager_provider_id: provider_id,
                manager_item_id: Some("1".to_string()),
                manager_label: Some("default (radarr)".to_string()),
                source: "find_media_add".to_string(),
            })
            .await?;
        let intent = store
            .list_active_managed_ingest_intents()
            .await?
            .into_iter()
            .find(|intent| intent.intent_id == intent_id)
            .expect("managed intent");
        let event = store
            .upsert_managed_import_event(&NewManagedImportEvent {
                event_key: "test-radarr-movie-metadata-event".to_string(),
                intent_id,
                media_type: MediaType::Movie,
                external_ids: Some(ExtIds {
                    imdb: Some("tt0381061".to_string()),
                    tmdb: Some("36557".to_string()),
                    ..Default::default()
                }),
                manager_provider_id: provider_id,
                manager_item_id: Some("1".to_string()),
                manager_label: Some("default (radarr)".to_string()),
                manager_implementation: Some("radarr".to_string()),
                imported_files: vec![ManagedImportFile {
                    path: file_path.to_string_lossy().to_string(),
                    season_number: None,
                    episode_number: None,
                    absolute_episode_number: None,
                    episode_title: None,
                    size_bytes: None,
                    container: Some("mkv".to_string()),
                    video_codec: Some("h264".to_string()),
                    audio_codec: Some("aac".to_string()),
                }],
                raw_manager_payload: None,
                imported_at: Some(Utc::now()),
            })
            .await?;

        let linked = ingest_managed_import_event(
            &database.pool,
            Some(&metadata),
            None,
            Some(&artwork),
            &intent,
            &event,
        )
        .await?;
        let _ = shutdown_tx.send(());
        assert!(linked.is_some());

        let row = sqlx::query("SELECT metadata_json, runtime_seconds FROM movies LIMIT 1")
            .fetch_one(&database.pool)
            .await?;
        let metadata_json: String = row.get("metadata_json");
        let metadata_json: serde_json::Value = serde_json::from_str(&metadata_json)?;
        assert_eq!(
            metadata_json.get("description").and_then(Value::as_str),
            Some("A managed Casino Royale description.")
        );
        assert_eq!(row.get::<i32, _>("runtime_seconds"), 144 * 60);

        let artwork_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM artwork_refs WHERE owner_type = 'movie' AND owner_id = $1",
        )
        .bind(linked.expect("linked movie").to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(artwork_count, 2);

        let status: String = sqlx::query_scalar("SELECT status FROM managed_import_events LIMIT 1")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(status, "linked");

        Ok(())
    }

    #[tokio::test]
    async fn managed_import_event_prefers_tvdb_movie_metadata_and_artwork() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let store = ExtensionStore::new(&database.pool);
        let provider_id = Uuid::new_v4();
        install_test_managed_provider(
            &database.pool,
            provider_id,
            "radarr",
            "media.manager.movies",
        )
        .await?;
        let file_dir = tempdir()?;
        let file_path = file_dir
            .path()
            .join("Casino Royale 2006 BluRay 1080p DDP 5 1 x264-hallowed.mkv");
        std::fs::write(&file_path, b"dummy")?;
        let artwork_dir = tempdir()?;
        let (cinemeta_base_url, cinemeta_shutdown_tx) = start_mock_cinemeta_server().await?;
        let (tvdb_base_url, tvdb_shutdown_tx) = start_mock_tvdb_movie_server().await?;

        let mut metadata_config = MetadataConfig::default();
        metadata_config.cinemeta_base_url = cinemeta_base_url;
        metadata_config.request_timeout_seconds = 2;
        let metadata = MetadataService::new(metadata_config)?;
        let mut classifier_config = ClassifierConfig::default();
        classifier_config.tvdb_base_url = tvdb_base_url;
        classifier_config.tvdb_api_key = Some("test-key".to_string());
        classifier_config.request_timeout_seconds = 2;
        let linkers = LinkerService::new(classifier_config)?;
        let artwork = ArtworkService::new(artwork_dir.path(), 2)?;

        let intent_id = store
            .upsert_managed_ingest_intent(&NewManagedIngestIntent {
                media_type: MediaType::Movie,
                title: "Casino Royale".to_string(),
                normalized_title: normalize_managed_intent_title("Casino Royale"),
                year: Some(2006),
                external_ids: Some(ExtIds {
                    imdb: Some("tt0381061".to_string()),
                    tmdb: Some("36557".to_string()),
                    ..Default::default()
                }),
                manager_provider_id: provider_id,
                manager_item_id: Some("1".to_string()),
                manager_label: Some("default (radarr)".to_string()),
                source: "find_media_add".to_string(),
            })
            .await?;
        let intent = store
            .list_active_managed_ingest_intents()
            .await?
            .into_iter()
            .find(|intent| intent.intent_id == intent_id)
            .expect("managed intent");
        let event = store
            .upsert_managed_import_event(&NewManagedImportEvent {
                event_key: "test-radarr-movie-tvdb-metadata-event".to_string(),
                intent_id,
                media_type: MediaType::Movie,
                external_ids: Some(ExtIds {
                    imdb: Some("tt0381061".to_string()),
                    tmdb: Some("36557".to_string()),
                    ..Default::default()
                }),
                manager_provider_id: provider_id,
                manager_item_id: Some("1".to_string()),
                manager_label: Some("default (radarr)".to_string()),
                manager_implementation: Some("radarr".to_string()),
                imported_files: vec![ManagedImportFile {
                    path: file_path.to_string_lossy().to_string(),
                    season_number: None,
                    episode_number: None,
                    absolute_episode_number: None,
                    episode_title: None,
                    size_bytes: None,
                    container: Some("mkv".to_string()),
                    video_codec: Some("h264".to_string()),
                    audio_codec: Some("aac".to_string()),
                }],
                raw_manager_payload: None,
                imported_at: Some(Utc::now()),
            })
            .await?;

        let linked = ingest_managed_import_event(
            &database.pool,
            Some(&metadata),
            Some(&linkers),
            Some(&artwork),
            &intent,
            &event,
        )
        .await?;
        let _ = cinemeta_shutdown_tx.send(());
        let _ = tvdb_shutdown_tx.send(());
        let movie_id = linked.expect("linked movie");

        let row = sqlx::query(
            "SELECT external_imdb, external_tmdb, metadata_json, runtime_seconds FROM movies LIMIT 1",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            row.try_get::<String, _>("external_imdb").ok().as_deref(),
            Some("tt0381061")
        );
        assert_eq!(
            row.try_get::<String, _>("external_tmdb").ok().as_deref(),
            Some("36557")
        );
        assert_eq!(row.get::<i32, _>("runtime_seconds"), 144 * 60);
        let metadata_json: String = row.get("metadata_json");
        let metadata_json: serde_json::Value = serde_json::from_str(&metadata_json)?;
        assert_eq!(
            metadata_json.get("overview").and_then(Value::as_str),
            Some("TVDB Casino Royale description.")
        );

        let tvdb_movie_id: String = sqlx::query_scalar(
            "SELECT external_id FROM movie_external_ids WHERE movie_id = $1 AND provider = 'tvdb' LIMIT 1",
        )
        .bind(movie_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(tvdb_movie_id, "12345");

        let tvdb_artwork_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM artwork_refs WHERE owner_type = 'movie' AND owner_id = $1 AND provider = 'tvdb'",
        )
        .bind(movie_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(tvdb_artwork_count, 2);

        let status: String = sqlx::query_scalar("SELECT status FROM managed_import_events LIMIT 1")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(status, "linked");

        Ok(())
    }

    #[tokio::test]
    async fn alm1_acquisition_import_prevalidates_all_episode_numbers_before_mutation() -> Result<()>
    {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let file_dir = tempdir()?;
        let valid_path = file_dir.path().join("Example Show S01E01.mkv");
        let missing_season_path = file_dir.path().join("Example Show - 13.mkv");
        std::fs::write(&valid_path, b"dummy")?;
        std::fs::write(&missing_season_path, b"dummy")?;

        let result = ingest_acquisition_library_import(
            &database.pool,
            AcquisitionLibraryImport {
                media_type: MediaType::Anime,
                title: "Example Show".to_string(),
                year: Some(2024),
                external_ids: ExtIds::default(),
                authority: None,
                files: vec![
                    AcquisitionLibraryImportFile {
                        path: valid_path.to_string_lossy().to_string(),
                        size_bytes: None,
                        season_number: Some(1),
                        episode_number: Some(1),
                        absolute_episode_number: Some(1),
                        episode_title: Some("First Episode".to_string()),
                    },
                    AcquisitionLibraryImportFile {
                        path: missing_season_path.to_string_lossy().to_string(),
                        size_bytes: None,
                        season_number: None,
                        episode_number: Some(13),
                        absolute_episode_number: Some(13),
                        episode_title: Some("Thirteenth Episode".to_string()),
                    },
                ],
            },
        )
        .await;

        let error = result.expect_err("missing season must reject the complete import");
        assert!(
            error.to_string().contains("missing a season number"),
            "unexpected error: {error:#}"
        );

        for table in [
            "series",
            "media_items",
            "media_files",
            "seasons",
            "episodes",
            "episode_files",
        ] {
            let query = format!("SELECT COUNT(*) FROM {table}");
            let count: i64 = sqlx::query_scalar(&query).fetch_one(&database.pool).await?;
            assert_eq!(count, 0, "{table} must remain unchanged after validation");
        }

        Ok(())
    }

    #[tokio::test]
    async fn alm1_verified_acquisition_sxxeyy_import_links_exact_episode() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let file_dir = tempdir()?;
        let file_path = file_dir.path().join("Example Show S02E01.mkv");
        std::fs::write(&file_path, b"dummy")?;

        let result = ingest_acquisition_library_import(
            &database.pool,
            AcquisitionLibraryImport {
                media_type: MediaType::Anime,
                title: "Example Show".to_string(),
                year: Some(2024),
                external_ids: ExtIds {
                    anilist: Some("12345".to_string()),
                    ..Default::default()
                },
                authority: None,
                files: vec![AcquisitionLibraryImportFile {
                    path: file_path.to_string_lossy().to_string(),
                    size_bytes: None,
                    season_number: Some(2),
                    episode_number: Some(1),
                    absolute_episode_number: Some(13),
                    episode_title: Some("Season Two Premiere".to_string()),
                }],
            },
        )
        .await?;

        assert_eq!(result.files.len(), 1);
        assert_eq!(result.files[0].season_number, Some(2));
        assert_eq!(result.files[0].episode_number, Some(1));

        let episode = sqlx::query(
            "SELECT e.season_number,
                    e.episode_number,
                    e.absolute_episode_number,
                    e.title,
                    CAST(e.has_file AS INTEGER) AS has_file
             FROM episodes e
             JOIN episode_files ef ON ef.episode_id = e.id
             WHERE ef.media_file_id = $1",
        )
        .bind(result.files[0].media_file_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(episode.get::<i32, _>("season_number"), 2);
        assert_eq!(episode.get::<i32, _>("episode_number"), 1);
        assert_eq!(episode.get::<i32, _>("absolute_episode_number"), 13);
        assert_eq!(
            episode.try_get::<String, _>("title").ok().as_deref(),
            Some("Season Two Premiere")
        );
        assert_eq!(episode.get::<i32, _>("has_file"), 1);

        let link_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM episode_files WHERE media_file_id = $1")
                .bind(result.files[0].media_file_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(link_count, 1);

        let release_id = Uuid::new_v4();
        let release_job_id = Uuid::new_v4();
        let import_run_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO acquisition_releases (
                release_id, source_extension_id, media_type, title, release_title, source,
                source_kind, fingerprint, release_kind, resolver_kind, resolver_version,
                confidence
             ) VALUES ($1, 'fixture.source', 'anime', 'Example Show',
                       'Example Show S02E01', 'fixture', 'torrent', $2, 'episode',
                       'deterministic', '1', 'verified')",
        )
        .bind(release_id.to_string())
        .bind(format!("fixture-{release_id}"))
        .execute(&database.pool)
        .await?;
        sqlx::query(
            "INSERT INTO acquisition_release_jobs (
                release_job_id, release_id, route_logical_id, state
             ) VALUES ($1, $2, 'fixture.route', 'completed')",
        )
        .bind(release_job_id.to_string())
        .bind(release_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query(
            "INSERT INTO acquisition_import_runs (
                import_run_id, release_id, release_job_id, route_logical_id, state
             ) VALUES ($1, $2, $3, 'fixture.route', 'completed')",
        )
        .bind(import_run_id.to_string())
        .bind(release_id.to_string())
        .bind(release_job_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query(
            "INSERT INTO acquisition_import_file_links (
                import_link_id, import_run_id, release_id, local_path, media_file_id,
                episode_id, state, verification_state
             ) VALUES ($1, $2, $3, $4, $5, $6, 'imported', 'verified')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(import_run_id.to_string())
        .bind(release_id.to_string())
        .bind(file_path.to_string_lossy().to_string())
        .bind(result.files[0].media_file_id.to_string())
        .bind(
            result.files[0]
                .episode_id
                .expect("verified import episode")
                .to_string(),
        )
        .execute(&database.pool)
        .await?;

        let classifier_calls = Arc::new(AtomicUsize::new(0));
        let noisy_pipeline = alm1_classifier_pipeline(
            alm1_hint(
                ClassifierLibraryType::Anime,
                "Incorrect Scanner Guess",
                Some(1),
                Some(99),
                Some(99),
            ),
            Arc::new(RwLock::new(vec![alm1_candidate(
                KindHint::Anime,
                "Incorrect Scanner Guess",
                Some("wrong-tvdb"),
                Some("wrong-anilist"),
                Some(1),
                Some(99),
                Some(99),
                1.0,
            )])),
            classifier_calls.clone(),
        );
        run_full_scan_with_classifier(
            &database.pool,
            None,
            None,
            None,
            None,
            &noisy_pipeline,
            vec![alm1_scan_candidate(
                &file_path.to_string_lossy(),
                MediaType::Series,
                "Incorrect Scanner Guess",
                Some(1999),
                Some(1),
                Some(99),
            )],
            false,
            true,
            false,
        )
        .await?;
        assert_eq!(
            classifier_calls.load(Ordering::SeqCst),
            0,
            "verified acquisition identity and numbering must bypass forced classification"
        );
        let rescanned_episode: (i64, i64, i64) = sqlx::query_as(
            "SELECT e.season_number, e.episode_number, e.absolute_episode_number
             FROM episodes e
             JOIN episode_files ef ON ef.episode_id = e.id
             WHERE ef.media_file_id = $1",
        )
        .bind(result.files[0].media_file_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(rescanned_episode, (2, 1, 13));
        let rescanned_link_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM episode_files WHERE media_file_id = $1")
                .bind(result.files[0].media_file_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(rescanned_link_count, 1);

        Ok(())
    }

    #[tokio::test]
    async fn acquisition_import_hydrates_series_metadata_and_artwork_from_tvdb() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let file_dir = tempdir()?;
        let file_path = file_dir.path().join("Star Wars Clone Wars 2003 S01E01.mkv");
        std::fs::write(&file_path, b"dummy")?;
        let artwork_dir = tempdir()?;
        let (tvdb_base_url, tvdb_shutdown_tx) = start_mock_tvdb_series_artwork_server().await?;

        let mut classifier_config = ClassifierConfig::default();
        classifier_config.tvdb_base_url = tvdb_base_url;
        classifier_config.tvdb_api_key = Some("test-key".to_string());
        classifier_config.request_timeout_seconds = 2;
        let linkers = LinkerService::new(classifier_config)?;
        let artwork = ArtworkService::new(artwork_dir.path(), 2)?;

        let result = ingest_acquisition_library_import_with_metadata(
            &database.pool,
            None,
            Some(&linkers),
            Some(&artwork),
            AcquisitionLibraryImport {
                media_type: MediaType::Series,
                title: "Star Wars: Clone Wars".to_string(),
                year: Some(2003),
                external_ids: ExtIds {
                    tvdb: Some("72244".to_string()),
                    tvdb_series: Some("72244".to_string()),
                    ..Default::default()
                },
                authority: None,
                files: vec![AcquisitionLibraryImportFile {
                    path: file_path.to_string_lossy().to_string(),
                    size_bytes: None,
                    season_number: Some(1),
                    episode_number: Some(1),
                    absolute_episode_number: Some(1),
                    episode_title: Some("Chapter I".to_string()),
                }],
            },
        )
        .await?;
        let _ = tvdb_shutdown_tx.send(());

        let row = sqlx::query(
            "SELECT external_tvdb_series, metadata_json FROM series WHERE id = $1 LIMIT 1",
        )
        .bind(result.media_item_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            row.try_get::<String, _>("external_tvdb_series")
                .ok()
                .as_deref(),
            Some("72244")
        );
        let metadata_json: String = row.get("metadata_json");
        let metadata_json: serde_json::Value = serde_json::from_str(&metadata_json)?;
        assert_eq!(
            metadata_json.get("overview").and_then(Value::as_str),
            Some("Animated micro-series following the Clone Wars.")
        );

        let season_row = sqlx::query(
            "SELECT title, metadata_json FROM seasons WHERE series_id = $1 AND season_number = 1 LIMIT 1",
        )
        .bind(result.media_item_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            season_row.try_get::<String, _>("title").ok().as_deref(),
            Some("Season 1")
        );
        let season_metadata: String = season_row.get("metadata_json");
        let season_metadata: serde_json::Value = serde_json::from_str(&season_metadata)?;
        assert_eq!(
            season_metadata
                .get("tvdb")
                .and_then(|value| value.get("name"))
                .and_then(Value::as_str),
            Some("Season 1")
        );

        let artwork_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM artwork_refs WHERE owner_type = 'series' AND owner_id = $1 AND provider = 'tvdb'",
        )
        .bind(result.media_item_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(artwork_count, 3);

        let cached_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM artwork_cache c INNER JOIN artwork_refs r ON r.id = c.artwork_id WHERE r.owner_type = 'series' AND r.owner_id = $1",
        )
        .bind(result.media_item_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert!(cached_count >= 3);

        Ok(())
    }

    #[tokio::test]
    async fn managed_series_import_event_links_episode_from_intent_identity() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let store = ExtensionStore::new(&database.pool);
        let provider_id = Uuid::new_v4();
        install_test_managed_provider(
            &database.pool,
            provider_id,
            "sonarr",
            "media.manager.series",
        )
        .await?;
        let file_dir = tempdir()?;
        let file_path = file_dir.path().join("Bad.Release.Name.S01E02.mkv");
        std::fs::write(&file_path, b"dummy")?;

        let intent_id = store
            .upsert_managed_ingest_intent(&NewManagedIngestIntent {
                media_type: MediaType::Series,
                title: "Example Show".to_string(),
                normalized_title: normalize_managed_intent_title("Example Show"),
                year: Some(2024),
                external_ids: Some(ExtIds {
                    imdb: Some("tt1234567".to_string()),
                    tvdb: Some("321".to_string()),
                    tvdb_series: Some("321".to_string()),
                    ..Default::default()
                }),
                manager_provider_id: provider_id,
                manager_item_id: Some("42".to_string()),
                manager_label: Some("default (sonarr)".to_string()),
                source: "find_media_add".to_string(),
            })
            .await?;
        let intent = store
            .list_active_managed_ingest_intents()
            .await?
            .into_iter()
            .find(|intent| intent.intent_id == intent_id)
            .expect("managed intent");
        let event = store
            .upsert_managed_import_event(&NewManagedImportEvent {
                event_key: "test-sonarr-series-event".to_string(),
                intent_id,
                media_type: MediaType::Series,
                external_ids: Some(ExtIds {
                    tvdb: Some("321".to_string()),
                    tvdb_series: Some("321".to_string()),
                    ..Default::default()
                }),
                manager_provider_id: provider_id,
                manager_item_id: Some("42".to_string()),
                manager_label: Some("default (sonarr)".to_string()),
                manager_implementation: Some("sonarr".to_string()),
                imported_files: vec![ManagedImportFile {
                    path: file_path.to_string_lossy().to_string(),
                    season_number: Some(1),
                    episode_number: Some(2),
                    absolute_episode_number: None,
                    episode_title: Some("Second Episode".to_string()),
                    size_bytes: None,
                    container: Some("mkv".to_string()),
                    video_codec: Some("h264".to_string()),
                    audio_codec: Some("aac".to_string()),
                }],
                raw_manager_payload: None,
                imported_at: Some(Utc::now()),
            })
            .await?;

        ingest_managed_import_event(&database.pool, None, None, None, &intent, &event).await?;

        let row = sqlx::query(
            "SELECT title, year, external_imdb, external_tvdb_series FROM series LIMIT 1",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(row.get::<String, _>("title"), "Example Show");
        assert_eq!(row.get::<i32, _>("year"), 2024);
        assert_eq!(
            row.try_get::<String, _>("external_imdb").ok().as_deref(),
            Some("tt1234567")
        );
        assert_eq!(
            row.try_get::<String, _>("external_tvdb_series")
                .ok()
                .as_deref(),
            Some("321")
        );
        let episode = sqlx::query(
            "SELECT season_number, episode_number, title, CAST(has_file AS INTEGER) AS has_file FROM episodes LIMIT 1",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(episode.get::<i32, _>("season_number"), 1);
        assert_eq!(episode.get::<i32, _>("episode_number"), 2);
        assert_eq!(
            episode.try_get::<String, _>("title").ok().as_deref(),
            Some("Second Episode")
        );
        assert_eq!(episode.get::<i32, _>("has_file"), 1);

        let status: String = sqlx::query_scalar("SELECT status FROM managed_import_events LIMIT 1")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(status, "linked");
        let matched: Option<String> = sqlx::query_scalar(
            "SELECT CAST(last_matched_at AS TEXT) FROM managed_ingest_intents LIMIT 1",
        )
        .fetch_one(&database.pool)
        .await?;
        assert!(matched.as_deref().is_some_and(|value| !value.is_empty()));

        let noisy_scan = vec![MediaFileCandidate {
            identity: MediaIdentity {
                r#type: MediaType::Series,
                external_ids: ExtIds::default(),
                title: "Bad Release Name".to_string(),
                year: None,
                season: Some(1),
                episode: Some(2),
            },
            files: vec![FD {
                path: file_path.to_string_lossy().to_string(),
                size_bytes: Some(2048),
                hash: None,
                container: Some("mkv".to_string()),
                video_codec: Some("h264".to_string()),
                audio_codec: Some("aac".to_string()),
            }],
            extension_metadata: HashMap::new(),
            source_config_id: None,
        }];
        run_full_scan(&database.pool, noisy_scan, true).await?;

        let (series_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM series")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(series_count, 1);
        let title: String = sqlx::query_scalar("SELECT title FROM series LIMIT 1")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(title, "Example Show");

        Ok(())
    }

    #[tokio::test]
    async fn managed_anime_intent_promotes_series_to_anime() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;

        let title = "Solo Leveling";
        let normalized_title = normalize_managed_intent_title(title);
        let provider_id = Uuid::new_v4();
        install_test_managed_provider(
            &database.pool,
            provider_id,
            "sonarr",
            "media.manager.series",
        )
        .await?;
        let external_ids_json = serde_json::json!({
            "anilist": "151807"
        });

        sqlx::query(
            "INSERT INTO managed_ingest_intents (
                intent_id, media_type, title, normalized_title, year, external_ids_json,
                manager_provider_id, manager_item_id, manager_label, source, active
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind("anime")
        .bind(title)
        .bind(normalized_title)
        .bind(2024)
        .bind(serde_json::to_string(&external_ids_json)?)
        .bind(provider_id.to_string())
        .bind("anime-151807")
        .bind("default(sonarr)")
        .bind("find_media_add")
        .execute(&database.pool)
        .await?;

        let candidates = vec![MediaFileCandidate {
            identity: MediaIdentity {
                r#type: MediaType::Series,
                external_ids: ExtIds::default(),
                title: title.to_string(),
                year: Some(2024),
                season: Some(1),
                episode: Some(1),
            },
            files: vec![FD {
                path: "/media/Solo.Leveling.S01E01.mkv".to_string(),
                size_bytes: Some(2048),
                hash: None,
                container: Some("mkv".to_string()),
                video_codec: Some("h264".to_string()),
                audio_codec: Some("aac".to_string()),
            }],
            extension_metadata: HashMap::new(),
            source_config_id: None,
        }];
        run_full_scan(&database.pool, candidates, true).await?;

        let row = sqlx::query("SELECT library_type, external_anilist FROM series LIMIT 1")
            .fetch_one(&database.pool)
            .await?;
        let library_type: String = row.try_get("library_type")?;
        let anilist: Option<String> = row.try_get("external_anilist")?;
        assert_eq!(library_type, "anime");
        assert_eq!(anilist.as_deref(), Some("151807"));

        let (pending_count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM review_queue WHERE status = 'pending'")
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(pending_count, 0);

        Ok(())
    }

    #[tokio::test]
    async fn local_acquisition_target_metadata_rehydrates_series_episode_catalog() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let media_item_id = Uuid::new_v4();
        let external_ids = ExtIds {
            imdb: Some("tt0361243".to_string()),
            tvdb: Some("72244".to_string()),
            tvdb_series: Some("72244".to_string()),
            ..Default::default()
        };
        let external_ids_json = serde_json::to_string(&external_ids)?;
        let series_metadata = serde_json::json!({
            "id": "tt0361243",
            "name": "Star Wars: Clone Wars",
            "videos": [
                {
                    "id": "tt0361243:1:1",
                    "season": 1,
                    "episode": 1,
                    "number": 1,
                    "name": "Chapter 1",
                    "thumbnail": "https://episodes.example.invalid/s1e1.jpg",
                    "tvdb_id": 79050
                },
                {
                    "id": "tt0361243:2:1",
                    "season": 2,
                    "episode": 1,
                    "number": 1,
                    "name": "Chapter 11",
                    "thumbnail": "https://episodes.example.invalid/s2e1.jpg",
                    "tvdb_id": 79060
                },
                {
                    "id": "tt0361243:3:1",
                    "season": 3,
                    "episode": 1,
                    "number": 1,
                    "name": "Chapter 21",
                    "overview": "Captain Fordo returns to Coruscant.",
                    "thumbnail": "https://episodes.example.invalid/s3e1.jpg",
                    "tvdb_id": 79070
                }
            ]
        });
        let series_metadata_json = serde_json::to_string(&series_metadata)?;

        sqlx::query(
            "INSERT INTO media_items (id, type, external_ids, title, year, metadata_json, created_at, updated_at)
             VALUES ($1, 'series', $2, 'Star Wars: Clone Wars', 2003, $3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .bind(media_item_id.to_string())
        .bind(&external_ids_json)
        .bind(&series_metadata_json)
        .execute(&database.pool)
        .await?;
        sqlx::query(
            "INSERT INTO series (id, title, year, library_type, external_imdb, external_tvdb_series, external_anilist, metadata_json, created_at, updated_at)
             VALUES ($1, 'Star Wars: Clone Wars', 2003, 'series', 'tt0361243', '72244', NULL, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .bind(media_item_id.to_string())
        .bind(&series_metadata_json)
        .execute(&database.pool)
        .await?;

        let existing_season_id = Uuid::new_v4();
        let existing_episode_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO seasons (id, series_id, season_number, created_at, updated_at)
             VALUES ($1, $2, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .bind(existing_season_id.to_string())
        .bind(media_item_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query(
            "INSERT INTO episodes (id, series_id, season_id, season_number, episode_number, title, metadata_json, has_file, created_at, updated_at)
             VALUES ($1, $2, $3, 1, 1, '', '', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .bind(existing_episode_id.to_string())
        .bind(media_item_id.to_string())
        .bind(existing_season_id.to_string())
        .execute(&database.pool)
        .await?;

        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Series,
                title: "Star Wars: Clone Wars".to_string(),
                year: Some(2003),
                external_ids: Some(external_ids),
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
        upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![
                NewAcquisitionTarget {
                    target_key: Some("S01E01".to_string()),
                    media_type: Some(MediaType::Series),
                    title: Some("Chapter I".to_string()),
                    season_number: Some(1),
                    episode_number: Some(1),
                    absolute_episode_number: Some(1),
                    air_date: None,
                    air_time: None,
                    metadata: Some(serde_json::json!({
                        "source": "tvdb",
                        "tvdbEpisodeId": "79050",
                        "raw": {
                            "id": 79050,
                            "name": "Chapter I",
                            "number": 1,
                            "seasonNumber": 1,
                            "absoluteNumber": 1,
                            "overview": "Like fire across the galaxy, the Clone Wars spread.",
                            "image": "https://artworks.example.invalid/s1e1.jpg",
                            "runtime": 4
                        }
                    })),
                    state: Some(AcquisitionTargetState::Imported),
                    next_search_after: None,
                },
                NewAcquisitionTarget {
                    target_key: Some("S02E01".to_string()),
                    media_type: Some(MediaType::Series),
                    title: Some("Chapter XI".to_string()),
                    season_number: Some(2),
                    episode_number: Some(1),
                    absolute_episode_number: Some(11),
                    air_date: None,
                    air_time: None,
                    metadata: Some(serde_json::json!({
                        "source": "tvdb",
                        "tvdbEpisodeId": "79060",
                        "raw": {
                            "id": 79060,
                            "name": "Chapter XI",
                            "number": 1,
                            "seasonNumber": 2,
                            "absoluteNumber": 11,
                            "overview": "Anakin continues his pursuit of Dark Jedi Asajj Ventress.",
                            "image": "https://artworks.example.invalid/s2e1.jpg",
                            "runtime": 4
                        }
                    })),
                    state: Some(AcquisitionTargetState::Pending),
                    next_search_after: None,
                },
                NewAcquisitionTarget {
                    target_key: Some("S03E01".to_string()),
                    media_type: Some(MediaType::Series),
                    title: Some("Chapter XXI".to_string()),
                    season_number: Some(3),
                    episode_number: Some(1),
                    absolute_episode_number: Some(21),
                    air_date: None,
                    air_time: None,
                    metadata: Some(serde_json::json!({
                        "mediaItemId": media_item_id,
                        "libraryEpisodeId": Uuid::new_v4(),
                        "acquisitionRequest": {
                            "mode": "one_shot",
                            "scope": "season",
                            "metadataPolicy": "initial_only"
                        }
                    })),
                    state: Some(AcquisitionTargetState::Pending),
                    next_search_after: None,
                },
            ],
        )
        .await?;

        let result =
            ensure_series_episode_catalog_from_local_metadata(&database.pool, None, media_item_id)
                .await?;
        assert_eq!(result.subscription_ids, vec![subscription.subscription_id]);

        let season_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM seasons WHERE series_id = $1")
                .bind(media_item_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(season_count, 3);

        let s1: (String, String, i64) = sqlx::query_as(
            "SELECT title, metadata_json, runtime_seconds
             FROM episodes
             WHERE series_id = $1 AND season_number = 1 AND episode_number = 1",
        )
        .bind(media_item_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(s1.0, "Chapter I");
        assert_eq!(s1.2, 240);
        let s1_metadata: Value = serde_json::from_str(&s1.1)?;
        assert_eq!(
            s1_metadata.get("overview").and_then(Value::as_str),
            Some("Like fire across the galaxy, the Clone Wars spread.")
        );

        let s2: (String, String, i64) = sqlx::query_as(
            "SELECT title, metadata_json, runtime_seconds
             FROM episodes
             WHERE series_id = $1 AND season_number = 2 AND episode_number = 1",
        )
        .bind(media_item_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(s2.0, "Chapter XI");
        assert_eq!(s2.2, 240);
        let s2_metadata: Value = serde_json::from_str(&s2.1)?;
        assert_eq!(
            s2_metadata.get("overview").and_then(Value::as_str),
            Some("Anakin continues his pursuit of Dark Jedi Asajj Ventress.")
        );

        let s3: (String, String) = sqlx::query_as(
            "SELECT title, metadata_json
             FROM episodes
             WHERE series_id = $1 AND season_number = 3 AND episode_number = 1",
        )
        .bind(media_item_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(s3.0, "Chapter 21");
        let s3_metadata: Value = serde_json::from_str(&s3.1)?;
        assert_eq!(
            s3_metadata.get("overview").and_then(Value::as_str),
            Some("Captain Fordo returns to Coruscant.")
        );
        assert!(
            s3_metadata.get("libraryEpisodeId").is_none(),
            "sparse one-shot target metadata must not replace richer episode metadata"
        );

        Ok(())
    }

    #[tokio::test]
    async fn managed_media_tombstone_blocks_reingest() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let store = ExtensionStore::new(&database.pool);

        store
            .upsert_managed_media_tombstone(&NewManagedMediaTombstone {
                media_type: MediaType::Movie,
                title: "Blocked Movie".to_string(),
                normalized_title: normalize_managed_intent_title("Blocked Movie"),
                year: Some(2024),
                external_ids: Some(ExtIds {
                    tmdb: Some("987".to_string()),
                    ..Default::default()
                }),
                manager_provider_id: None,
                manager_item_id: None,
                manager_label: None,
                manager_implementation: None,
                action: "stop_tracking".to_string(),
            })
            .await?;

        let candidates = vec![MediaFileCandidate {
            identity: MediaIdentity {
                r#type: MediaType::Movie,
                external_ids: ExtIds {
                    tmdb: Some("987".to_string()),
                    ..Default::default()
                },
                title: "Blocked Movie".to_string(),
                year: Some(2024),
                season: None,
                episode: None,
            },
            files: vec![FD {
                path: "/media/blocked_movie.mkv".to_string(),
                size_bytes: Some(2048),
                hash: None,
                container: Some("mkv".to_string()),
                video_codec: Some("h264".to_string()),
                audio_codec: Some("aac".to_string()),
            }],
            extension_metadata: HashMap::new(),
            source_config_id: None,
        }];
        run_full_scan(&database.pool, candidates, false).await?;

        let (movie_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM movies")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(movie_count, 0);

        let (legacy_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM media_items")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(legacy_count, 0);

        Ok(())
    }

    #[tokio::test]
    async fn managed_episode_tombstone_blocks_reingest() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let store = ExtensionStore::new(&database.pool);

        store
            .upsert_managed_episode_tombstone(
                &crate::extensions::store::NewManagedEpisodeTombstone {
                    media_type: MediaType::Series,
                    title: "Blocked Show".to_string(),
                    normalized_title: normalize_managed_intent_title("Blocked Show"),
                    year: Some(2024),
                    external_ids: Some(ExtIds {
                        tvdb_series: Some("321".to_string()),
                        tvdb: Some("321".to_string()),
                        ..Default::default()
                    }),
                    manager_provider_id: None,
                    manager_item_id: None,
                    manager_label: None,
                    manager_implementation: None,
                    season_number: 1,
                    episode_number: 2,
                    absolute_episode_number: None,
                    action: "block_episode".to_string(),
                },
            )
            .await?;

        let candidates = vec![MediaFileCandidate {
            identity: MediaIdentity {
                r#type: MediaType::Series,
                external_ids: ExtIds {
                    tvdb_series: Some("321".to_string()),
                    tvdb: Some("321".to_string()),
                    ..Default::default()
                },
                title: "Blocked Show".to_string(),
                year: Some(2024),
                season: Some(1),
                episode: Some(2),
            },
            files: vec![FD {
                path: "/media/Blocked.Show.S01E02.mkv".to_string(),
                size_bytes: Some(2048),
                hash: None,
                container: Some("mkv".to_string()),
                video_codec: Some("h264".to_string()),
                audio_codec: Some("aac".to_string()),
            }],
            extension_metadata: HashMap::new(),
            source_config_id: None,
        }];
        run_full_scan(&database.pool, candidates, false).await?;

        let (series_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM series")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(series_count, 1);

        let (legacy_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM media_items")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(legacy_count, 1);

        let (media_file_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM media_files")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(media_file_count, 0);

        let (episode_link_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM episode_files")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(episode_link_count, 0);

        Ok(())
    }

    #[derive(Debug, Clone, Copy)]
    struct EpisodeRelinkFixture {
        series_id: Uuid,
        old_episode_id: Uuid,
        target_episode_id: Uuid,
        media_file_id: Uuid,
        movie_id: Uuid,
    }

    async fn seed_episode_relink_fixture(pool: &AnyPool) -> Result<EpisodeRelinkFixture> {
        let series_id = Uuid::new_v4();
        let season_id = Uuid::new_v4();
        let fixture = EpisodeRelinkFixture {
            series_id,
            old_episode_id: Uuid::new_v4(),
            target_episode_id: Uuid::new_v4(),
            media_file_id: Uuid::new_v4(),
            movie_id: Uuid::new_v4(),
        };

        sqlx::query::<sqlx::Any>(
            "INSERT INTO media_items (id, type, title) VALUES ($1, 'anime', 'Relink Series')",
        )
        .bind(series_id.to_string())
        .execute(pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO series (id, title, library_type) VALUES ($1, 'Relink Series', 'anime')",
        )
        .bind(series_id.to_string())
        .execute(pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO seasons (id, series_id, season_number) VALUES ($1, $2, 1)",
        )
        .bind(season_id.to_string())
        .bind(series_id.to_string())
        .execute(pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO episodes \
             (id, series_id, season_id, season_number, episode_number, has_file) \
             VALUES ($1, $2, $3, 1, 1, TRUE), ($4, $2, $3, 1, 2, FALSE)",
        )
        .bind(fixture.old_episode_id.to_string())
        .bind(series_id.to_string())
        .bind(season_id.to_string())
        .bind(fixture.target_episode_id.to_string())
        .execute(pool)
        .await?;

        sqlx::query::<sqlx::Any>(
            "INSERT INTO media_items (id, type, title) VALUES ($1, 'movie', 'Stale Movie')",
        )
        .bind(fixture.movie_id.to_string())
        .execute(pool)
        .await?;
        sqlx::query::<sqlx::Any>("INSERT INTO movies (id, title) VALUES ($1, 'Stale Movie')")
            .bind(fixture.movie_id.to_string())
            .execute(pool)
            .await?;

        sqlx::query::<sqlx::Any>(
            "INSERT INTO media_files (id, media_item_id, path, scan_state) \
             VALUES ($1, $2, $3, 'ok')",
        )
        .bind(fixture.media_file_id.to_string())
        .bind(series_id.to_string())
        .bind(format!("/media/{}.mkv", fixture.media_file_id))
        .execute(pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO episode_files (episode_id, media_file_id) VALUES ($1, $2)",
        )
        .bind(fixture.old_episode_id.to_string())
        .bind(fixture.media_file_id.to_string())
        .execute(pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO movie_files (movie_id, media_file_id) VALUES ($1, $2)",
        )
        .bind(fixture.movie_id.to_string())
        .bind(fixture.media_file_id.to_string())
        .execute(pool)
        .await?;

        Ok(fixture)
    }

    #[tokio::test]
    async fn alm1_episode_relink_replaces_stale_links_and_is_idempotent() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let fixture = seed_episode_relink_fixture(&database.pool).await?;

        link_episode_file(
            &database.pool,
            fixture.target_episode_id,
            fixture.media_file_id,
        )
        .await?;

        let linked_episode_ids: Vec<String> = sqlx::query_scalar(
            "SELECT episode_id FROM episode_files WHERE media_file_id = $1 ORDER BY episode_id",
        )
        .bind(fixture.media_file_id.to_string())
        .fetch_all(&database.pool)
        .await?;
        assert_eq!(
            linked_episode_ids,
            vec![fixture.target_episode_id.to_string()]
        );

        let old_has_file: i64 =
            sqlx::query_scalar("SELECT CAST(has_file AS INTEGER) FROM episodes WHERE id = $1")
                .bind(fixture.old_episode_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        let target_has_file: i64 =
            sqlx::query_scalar("SELECT CAST(has_file AS INTEGER) FROM episodes WHERE id = $1")
                .bind(fixture.target_episode_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(old_has_file, 0);
        assert_eq!(target_has_file, 1);

        let movie_link_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM movie_files WHERE media_file_id = $1")
                .bind(fixture.media_file_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        let movie_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM movies WHERE id = $1")
            .bind(fixture.movie_id.to_string())
            .fetch_one(&database.pool)
            .await?;
        let legacy_movie_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM media_items WHERE id = $1")
                .bind(fixture.movie_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(movie_link_count, 0);
        assert_eq!(movie_count, 1);
        assert_eq!(legacy_movie_count, 1);

        link_episode_file(
            &database.pool,
            fixture.target_episode_id,
            fixture.media_file_id,
        )
        .await?;
        let link_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM episode_files WHERE media_file_id = $1")
                .bind(fixture.media_file_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        let linked_episode_id: String =
            sqlx::query_scalar("SELECT episode_id FROM episode_files WHERE media_file_id = $1")
                .bind(fixture.media_file_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(link_count, 1);
        assert_eq!(linked_episode_id, fixture.target_episode_id.to_string());

        Ok(())
    }

    #[tokio::test]
    async fn alm8_forward_relink_removes_only_exact_internal_movie_placeholder() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let fixture = seed_episode_relink_fixture(&database.pool).await?;
        let marker = "{\"classifierPlaceholder\":true}";
        sqlx::query::<sqlx::Any>("UPDATE movies SET metadata_json = $1 WHERE id = $2")
            .bind(marker)
            .bind(fixture.movie_id.to_string())
            .execute(&database.pool)
            .await?;
        sqlx::query::<sqlx::Any>(
            "UPDATE media_items SET metadata_json = $1, external_ids = '{}' WHERE id = $2",
        )
        .bind(marker)
        .bind(fixture.movie_id.to_string())
        .execute(&database.pool)
        .await?;

        link_episode_file(
            &database.pool,
            fixture.target_episode_id,
            fixture.media_file_id,
        )
        .await?;

        let counts: (i64, i64, i64) = sqlx::query_as(
            "SELECT \
             (SELECT COUNT(*) FROM movie_files WHERE media_file_id = $1), \
             (SELECT COUNT(*) FROM movies WHERE id = $2), \
             (SELECT COUNT(*) FROM media_items WHERE id = $2)",
        )
        .bind(fixture.media_file_id.to_string())
        .bind(fixture.movie_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(counts, (0, 0, 0));
        Ok(())
    }

    #[tokio::test]
    async fn alm1_episode_relink_rolls_back_every_change_on_insert_failure() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let fixture = seed_episode_relink_fixture(&database.pool).await?;

        let trigger = format!(
            "CREATE TRIGGER alm1_relink_failure \
             BEFORE INSERT ON episode_files \
             WHEN NEW.episode_id = '{}' \
             BEGIN SELECT RAISE(ABORT, 'forced episode relink failure'); END",
            fixture.target_episode_id
        );
        sqlx::query::<sqlx::Any>(&trigger)
            .execute(&database.pool)
            .await?;

        let error = link_episode_file(
            &database.pool,
            fixture.target_episode_id,
            fixture.media_file_id,
        )
        .await
        .expect_err("trigger must abort the replacement link");
        assert!(
            error.to_string().contains("forced episode relink failure"),
            "unexpected relink error: {error:#}"
        );

        let linked_episode_ids: Vec<String> = sqlx::query_scalar(
            "SELECT episode_id FROM episode_files WHERE media_file_id = $1 ORDER BY episode_id",
        )
        .bind(fixture.media_file_id.to_string())
        .fetch_all(&database.pool)
        .await?;
        assert_eq!(linked_episode_ids, vec![fixture.old_episode_id.to_string()]);

        let movie_link_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM movie_files WHERE media_file_id = $1")
                .bind(fixture.media_file_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        let movie_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM movies WHERE id = $1")
            .bind(fixture.movie_id.to_string())
            .fetch_one(&database.pool)
            .await?;
        let legacy_movie_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM media_items WHERE id = $1")
                .bind(fixture.movie_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(movie_link_count, 1);
        assert_eq!(movie_count, 1);
        assert_eq!(legacy_movie_count, 1);

        let old_has_file: i64 =
            sqlx::query_scalar("SELECT CAST(has_file AS INTEGER) FROM episodes WHERE id = $1")
                .bind(fixture.old_episode_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        let target_has_file: i64 =
            sqlx::query_scalar("SELECT CAST(has_file AS INTEGER) FROM episodes WHERE id = $1")
                .bind(fixture.target_episode_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(old_has_file, 1);
        assert_eq!(target_has_file, 0);
        let media_item_id: String =
            sqlx::query_scalar("SELECT media_item_id FROM media_files WHERE id = $1")
                .bind(fixture.media_file_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(media_item_id, fixture.series_id.to_string());

        Ok(())
    }

    #[tokio::test]
    async fn alm1_concurrent_movie_episode_relinks_keep_identity_and_link_consistent() -> Result<()>
    {
        let temp = tempdir()?;
        let db_path = temp.path().join("alm1-concurrent-relink.db");
        let config = DatabaseConfig {
            url: format!("sqlite://{}", db_path.display()),
            max_connections: 4,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let series_id = Uuid::new_v4();
        let season_id = Uuid::new_v4();
        let episode_id = Uuid::new_v4();
        let movie_id = Uuid::new_v4();
        let media_file_id = Uuid::new_v4();

        sqlx::query("INSERT INTO media_items (id, type, title) VALUES ($1, 'anime', 'Race')")
            .bind(series_id.to_string())
            .execute(&database.pool)
            .await?;
        sqlx::query("INSERT INTO series (id, title, library_type) VALUES ($1, 'Race', 'anime')")
            .bind(series_id.to_string())
            .execute(&database.pool)
            .await?;
        sqlx::query("INSERT INTO seasons (id, series_id, season_number) VALUES ($1, $2, 1)")
            .bind(season_id.to_string())
            .bind(series_id.to_string())
            .execute(&database.pool)
            .await?;
        sqlx::query(
            "INSERT INTO episodes \
             (id, series_id, season_id, season_number, episode_number) \
             VALUES ($1, $2, $3, 1, 1)",
        )
        .bind(episode_id.to_string())
        .bind(series_id.to_string())
        .bind(season_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query("INSERT INTO media_items (id, type, title) VALUES ($1, 'movie', 'Race Movie')")
            .bind(movie_id.to_string())
            .execute(&database.pool)
            .await?;
        sqlx::query("INSERT INTO movies (id, title) VALUES ($1, 'Race Movie')")
            .bind(movie_id.to_string())
            .execute(&database.pool)
            .await?;
        sqlx::query(
            "INSERT INTO media_files (id, media_item_id, path, scan_state) \
             VALUES ($1, $2, '/media/alm1-race.mkv', 'ok')",
        )
        .bind(media_file_id.to_string())
        .bind(series_id.to_string())
        .execute(&database.pool)
        .await?;

        let episode_link = link_episode_file(&database.pool, episode_id, media_file_id);
        let movie_link = link_movie_file(&database.pool, movie_id, media_file_id);
        let (episode_result, movie_result) = tokio::join!(episode_link, movie_link);
        episode_result?;
        movie_result?;

        let movie_link_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM movie_files WHERE media_file_id = $1")
                .bind(media_file_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        let episode_link_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM episode_files WHERE media_file_id = $1")
                .bind(media_file_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(movie_link_count + episode_link_count, 1);
        let media_item_id: String =
            sqlx::query_scalar("SELECT media_item_id FROM media_files WHERE id = $1")
                .bind(media_file_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        if movie_link_count == 1 {
            assert_eq!(media_item_id, movie_id.to_string());
        } else {
            assert_eq!(media_item_id, series_id.to_string());
        }

        Ok(())
    }
}
