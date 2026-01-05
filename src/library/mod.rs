mod linkers;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use elixir_classifier::pipeline::{ClassifiedHint, ClassifierPipeline};
use elixir_classifier::hint::{
    ClassificationHint as ClassifierHint, FileInput as ClassifierFileInput,
    LibraryType as ClassifierLibraryType,
};
use elixir_classifier::hint::anime_parser_adapter::AnimeParserAdapter;
use elixir_classifier::hint::folder_context_parser::FolderContextParser;
use elixir_classifier::hint::general_parser::GeneralParser;
use elixir_classifier::hint::id_extractor_parser::IdExtractorParser;
use elixir_classifier::identify::anilist::AniListIdentifier;
use elixir_classifier::identify::cinemeta::CinemetaIdentifier;
use elixir_classifier::identify::{CanonicalMatch as ClassifierCanonicalMatch, ExternalIds as ClassifierExternalIds};
use elixir_classifier::link::anizip_linker::AniZipLinker;
use elixir_classifier::link::tvdb_linker::TvdbLinker;
use sqlx::{AnyPool, Row};
use uuid::Uuid;

use crate::{
    config::ClassifierConfig,
    db::models::MediaType,
    extensions::{FileDescriptor, MediaFileCandidate, MediaIdentity},
    extensions::{ExternalIds, make_identity_key},
    media::ffprobe,
    metadata::{MetadataResult, MetadataService},
    state::AppState,
};

pub use linkers::LinkerService;
use linkers::{AniZipEpisodeRecord, AniZipMapping};

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
        candidates,
        false,
        hash_dedupe,
    )
    .await
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
        candidates,
        force_metadata,
        hash_dedupe,
    )
    .await
}

pub async fn run_full_scan_with_metadata_and_linkers(
    pool: &AnyPool,
    metadata: Option<&MetadataService>,
    linkers: Option<&LinkerService>,
    classifier_config: Option<&ClassifierConfig>,
    candidates: Vec<MediaFileCandidate>,
    force_metadata: bool,
    hash_dedupe: bool,
) -> Result<()> {
    let (merged, mut seen_paths): (Vec<AggregatedCandidate>, HashSet<String>) =
        merge_candidates(candidates, hash_dedupe);
    let classifier = build_classifier_pipeline(classifier_config);

    for candidate in merged {
        let mut identity_for_meta = candidate.identity.clone();
        identity_for_meta.season = None;
        identity_for_meta.episode = None;

        let meta = if let Some(service) = metadata {
            let should_refresh = should_refresh_metadata(
                pool,
                &identity_for_meta,
                service.ttl_seconds(),
                force_metadata,
            )
            .await?;
            if should_refresh {
                service
                    .fetch_metadata(&identity_for_meta)
                    .await
                    .ok()
                    .flatten()
            } else {
                None
            }
        } else {
            None
        };

        let merged_ids = merge_external_ids(
            &candidate.identity.external_ids,
            meta.as_ref().and_then(|m| m.external_ids.clone()),
        );

        let (merged_ids, review_outcomes) = classify_candidate_files(
            pool,
            &classifier,
            &candidate,
            &merged_ids,
        )
        .await?;
        let mut merged_ids = merged_ids;

        let mut anizip_mapping: Option<AniZipMapping> = None;
        if let Some(linker) = linkers {
            if matches!(
                candidate.identity.r#type,
                MediaType::Series | MediaType::Anime
            ) {
                if merged_ids.tvdb_series.is_none() {
                    if let Some(imdb) = merged_ids.imdb.as_ref() {
                        if let Ok(Some(tvdb_id)) = linker.link_tvdb_series_by_imdb(imdb).await {
                            merged_ids.tvdb_series = Some(tvdb_id);
                        }
                    }
                }
                if matches!(candidate.identity.r#type, MediaType::Anime) {
                    if let Some(anilist) = merged_ids.anilist.as_ref() {
                        if let Ok(Some(mapping)) = linker.fetch_anizip_mapping(anilist).await {
                            merged_ids = merge_external_ids(&merged_ids, Some(mapping.ids.clone()));
                            anizip_mapping = Some(mapping);
                        }
                    }
                }
            }
        }

        match candidate.identity.r#type {
            MediaType::Movie => {
                let movie_id =
                    upsert_movie(pool, &candidate.identity, &merged_ids, meta.as_ref()).await?;
                upsert_legacy_media_item(
                    pool,
                    movie_id,
                    &candidate.identity,
                    &merged_ids,
                    meta.as_ref(),
                )
                .await?;
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
                    if let Some(outcome) = review_outcomes.get(&file.descriptor.path) {
                        persist_review_outcome(pool, media_file.id, outcome).await?;
                    }
                    link_movie_file(pool, movie_id, media_file.id).await?;
                    if let Some(duration) = media_file.duration_seconds {
                        update_movie_runtime_if_missing(pool, movie_id, duration).await?;
                    }
                }
            }
            MediaType::Series | MediaType::Anime => {
                let series_id =
                    upsert_series(pool, &candidate.identity, &merged_ids, meta.as_ref()).await?;
                upsert_legacy_media_item(
                    pool,
                    series_id,
                    &candidate.identity,
                    &merged_ids,
                    meta.as_ref(),
                )
                .await?;
                persist_series_external_ids(pool, series_id, &merged_ids, "classifier").await?;

                let mut resolved_numbers: HashMap<String, ResolvedEpisodeNumbers> = HashMap::new();
                for file in &candidate.files {
                    let outcome = review_outcomes.get(&file.descriptor.path);
                    let resolved = resolve_episode_numbers(
                        file,
                        outcome,
                        candidate.identity.r#type,
                        anizip_mapping.as_ref(),
                    );
                    resolved_numbers.insert(file.descriptor.path.clone(), resolved);
                }

                let mut season_ids: HashMap<i32, Uuid> = HashMap::new();
                for file in &candidate.files {
                    let resolved = resolved_numbers
                        .get(&file.descriptor.path)
                        .copied()
                        .unwrap_or(ResolvedEpisodeNumbers {
                            season: file.season,
                            episode: file.episode,
                            absolute_episode: file.absolute_episode,
                        });
                    let season_number = resolved.season.unwrap_or(1);
                    if !season_ids.contains_key(&season_number) {
                        let season_id = upsert_season(pool, series_id, season_number).await?;
                        season_ids.insert(season_number, season_id);
                    }
                }

                if let Some(linker) = linkers {
                    for (season_number, season_id) in &season_ids {
                        if matches!(candidate.identity.r#type, MediaType::Anime) {
                            let mapping = if let Some(existing) = anizip_mapping.as_ref() {
                                Some(existing)
                            } else if let Some(anilist) = merged_ids.anilist.as_ref() {
                                if let Ok(Some(fetched)) =
                                    linker.fetch_anizip_mapping(anilist).await
                                {
                                    anizip_mapping = Some(fetched);
                                    anizip_mapping.as_ref()
                                } else {
                                    None
                                }
                            } else {
                                None
                            };
                            if let Some(mapping) = mapping {
                                ensure_anizip_season_scaffold(
                                    pool,
                                    series_id,
                                    *season_id,
                                    *season_number,
                                    mapping,
                                )
                                .await?;
                            }
                        } else if let Some(tvdb_id) = merged_ids.tvdb_series.as_ref() {
                            ensure_tvdb_season_scaffold(
                                pool,
                                series_id,
                                *season_id,
                                tvdb_id,
                                *season_number,
                                linker,
                            )
                            .await?;
                        }
                    }
                }

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
                    if let Some(outcome) = review_outcomes.get(&file.descriptor.path) {
                        persist_review_outcome(pool, media_file.id, outcome).await?;
                    }
                    let resolved = resolved_numbers
                        .get(&file.descriptor.path)
                        .copied()
                        .unwrap_or(ResolvedEpisodeNumbers {
                            season: file.season,
                            episode: file.episode,
                            absolute_episode: file.absolute_episode,
                        });
                    let season_number = resolved.season.unwrap_or(1);
                    let episode_number = resolved.episode.unwrap_or(1);
                    let season_id = season_ids
                        .get(&season_number)
                        .copied()
                        .unwrap_or(upsert_season(pool, series_id, season_number).await?);
                    let episode_id = upsert_episode(
                        pool,
                        series_id,
                        season_id,
                        season_number,
                        episode_number,
                        resolved.absolute_episode,
                    )
                    .await?;
                    link_episode_file(pool, episode_id, media_file.id).await?;
                    mark_episode_has_file(pool, episode_id).await?;
                    if let Some(duration) = media_file.duration_seconds {
                        update_episode_runtime_if_missing(pool, episode_id, duration).await?;
                    }
                }
            }
        }
    }

    // Mark missing
    let existing_paths: Vec<String> = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT path FROM media_files WHERE scan_state = 'ok'",
    )
    .fetch_all(pool)
    .await?;

    for path in existing_paths {
        if !seen_paths.contains(&path) {
            sqlx::query::<sqlx::Any>(
                "UPDATE media_files SET scan_state = 'missing' WHERE path = ?",
            )
            .bind(path)
            .execute(pool)
            .await?;
        }
    }

    refresh_episode_file_state(pool).await?;

    Ok(())
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
            entry.identity.external_ids =
                merge_external_ids(&entry.identity.external_ids, Some(candidate.identity.external_ids.clone()));
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReviewQueueStatus {
    Pending,
    Applied,
}

impl ReviewQueueStatus {
    fn as_str(&self) -> &'static str {
        match self {
            ReviewQueueStatus::Pending => "pending",
            ReviewQueueStatus::Applied => "applied",
        }
    }
}

#[derive(Debug, Clone)]
struct ReviewOutcome {
    status: ReviewQueueStatus,
    confidence: Option<f32>,
    hint_json: Option<String>,
    candidates_json: Option<String>,
    parsed_hint: Option<ClassifierHint>,
}

#[derive(Debug, Clone, Copy)]
struct ResolvedEpisodeNumbers {
    season: Option<i32>,
    episode: Option<i32>,
    absolute_episode: Option<i32>,
}

async fn classify_candidate_files(
    pool: &AnyPool,
    classifier: &ClassifierPipeline,
    candidate: &AggregatedCandidate,
    merged_ids: &ExternalIds,
) -> Result<(ExternalIds, HashMap<String, ReviewOutcome>)> {
    let library_type = candidate.identity.r#type;
    let library_type_key = library_type_string(library_type);
    let mut updated_ids = merged_ids.clone();
    let mut outcomes: HashMap<String, ReviewOutcome> = HashMap::new();
    let mut override_cache: HashMap<String, Option<ExternalIds>> = HashMap::new();

    for file in &candidate.files {
        let path = &file.descriptor.path;
        if let Some(override_ids) =
            lookup_override_for_path(pool, library_type_key, path, &mut override_cache).await?
        {
            updated_ids = merge_external_ids(&updated_ids, Some(override_ids));
            outcomes.insert(
                path.clone(),
                ReviewOutcome {
                    status: ReviewQueueStatus::Applied,
                    confidence: None,
                    hint_json: None,
                    candidates_json: None,
                    parsed_hint: None,
                },
            );
            continue;
        }

        if has_strong_ids(library_type, &updated_ids) {
            outcomes.insert(
                path.clone(),
                ReviewOutcome {
                    status: ReviewQueueStatus::Applied,
                    confidence: None,
                    hint_json: None,
                    candidates_json: None,
                    parsed_hint: None,
                },
            );
            continue;
        }

        let input = build_classifier_input(file, library_type, &updated_ids);
        let results = classifier.classify_file(&input).await?;
        let selection = select_best_classification(results);
        let outcome = match selection {
            Some((hint, canonical)) => {
                let decision = review_decision_from_match(canonical.as_ref());
                let review_recommended = canonical
                    .as_ref()
                    .map(|c| c.confidence >= 0.65 && c.confidence < 0.85)
                    .unwrap_or(false);
                let (hint_json, candidates_json) =
                    build_review_payloads(&hint, canonical.as_ref(), review_recommended)?;
                if let Some(canonical) = canonical.as_ref() {
                    if canonical.confidence >= 0.65 {
                        let mapped = classifier_ids_to_server(&canonical.ids, library_type);
                        updated_ids = merge_external_ids(&updated_ids, Some(mapped));
                    }
                }
                ReviewOutcome {
                    status: decision,
                    confidence: canonical.as_ref().map(|c| c.confidence),
                    hint_json,
                    candidates_json,
                    parsed_hint: Some(hint.clone()),
                }
            }
            None => ReviewOutcome {
                status: ReviewQueueStatus::Pending,
                confidence: None,
                hint_json: None,
                candidates_json: None,
                parsed_hint: None,
            },
        };
        outcomes.insert(path.clone(), outcome);
    }

    Ok((updated_ids, outcomes))
}

fn resolve_episode_numbers(
    file: &AggregatedFile,
    outcome: Option<&ReviewOutcome>,
    media_type: MediaType,
    anizip_mapping: Option<&AniZipMapping>,
) -> ResolvedEpisodeNumbers {
    let mut season = file.season;
    let mut episode = file.episode;
    let mut absolute_episode = file.absolute_episode;

    if let Some(outcome) = outcome {
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
    }

    if matches!(media_type, MediaType::Anime) {
        if (season.is_none() || episode.is_none()) {
            if let Some(abs) = absolute_episode {
                if let Some((mapped_season, mapped_episode)) =
                    lookup_anizip_absolute_episode(anizip_mapping, abs)
                {
                    if season.is_none() {
                        season = Some(mapped_season);
                    }
                    if episode.is_none() {
                        episode = Some(mapped_episode);
                    }
                }
            }
        }
    }

    ResolvedEpisodeNumbers {
        season,
        episode,
        absolute_episode,
    }
}

fn lookup_anizip_absolute_episode(
    mapping: Option<&AniZipMapping>,
    absolute_episode: i32,
) -> Option<(i32, i32)> {
    let mapping = mapping?;
    mapping
        .episodes
        .iter()
        .find(|episode| episode.absolute_episode_number == Some(absolute_episode))
        .and_then(|episode| {
            let season = episode.season_number?;
            let number = episode.episode_number?;
            Some((season, number))
        })
}

fn review_decision_from_match(match_opt: Option<&ClassifierCanonicalMatch>) -> ReviewQueueStatus {
    match match_opt {
        Some(matched) if matched.confidence >= 0.85 => ReviewQueueStatus::Applied,
        Some(_) => ReviewQueueStatus::Pending,
        None => ReviewQueueStatus::Pending,
    }
}

fn build_review_payloads(
    hint: &elixir_classifier::hint::ClassificationHint,
    canonical: Option<&ClassifierCanonicalMatch>,
    review_recommended: bool,
) -> Result<(Option<String>, Option<String>)> {
    let mut hint_value = serde_json::to_value(hint)?;
    if review_recommended {
        if let Some(obj) = hint_value.as_object_mut() {
            obj.insert("reviewRecommended".to_string(), serde_json::Value::Bool(true));
        }
    }
    let hint_json = Some(serde_json::to_string(&hint_value)?);

    let candidates_json = match canonical {
        Some(canonical) => {
            let value = serde_json::json!({ "candidates": canonical.considered });
            Some(serde_json::to_string(&value)?)
        }
        None => None,
    };

    Ok((hint_json, candidates_json))
}

fn select_best_classification(
    results: Vec<ClassifiedHint>,
) -> Option<(elixir_classifier::hint::ClassificationHint, Option<ClassifierCanonicalMatch>)> {
    let mut best: Option<(elixir_classifier::hint::ClassificationHint, Option<ClassifierCanonicalMatch>)> = None;
    for item in results {
        match (&best, &item.canonical) {
            (None, _) => best = Some((item.hint, item.canonical)),
            (Some((_hint, Some(current))), Some(candidate)) => {
                if candidate.confidence > current.confidence {
                    best = Some((item.hint, item.canonical));
                }
            }
            (Some((_hint, None)), Some(_)) => {
                best = Some((item.hint, item.canonical));
            }
            _ => {}
        }
    }
    best
}

fn build_classifier_pipeline(classifier_config: Option<&ClassifierConfig>) -> ClassifierPipeline {
    let config = classifier_config.cloned().unwrap_or_default();
    let tvdb_linker = TvdbLinker::new(
        config.tvdb_base_url,
        config.tvdb_api_key,
        config.request_timeout_seconds,
    );
    ClassifierPipeline::new()
        .register_hint_parser(Arc::new(GeneralParser::default()))
        .register_hint_parser(Arc::new(IdExtractorParser::default()))
        .register_hint_parser(Arc::new(FolderContextParser::default()))
        .register_hint_parser(Arc::new(AnimeParserAdapter::default()))
        .register_identifier_provider(Arc::new(CinemetaIdentifier::default()))
        .register_identifier_provider(Arc::new(AniListIdentifier::default()))
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
        MediaType::Movie => (
            None,
            ids.tvdb_movie.clone().or_else(|| ids.tvdb.clone()),
        ),
        _ => (
            ids.tvdb_series.clone().or_else(|| ids.tvdb.clone()),
            None,
        ),
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
        MediaType::Movie => (None, ids.tvdb_movie.clone().or_else(|| ids.tvdb_series.clone())),
        _ => (ids.tvdb_series.clone().or_else(|| ids.tvdb_movie.clone()), None),
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
        MediaType::Movie => {
            ids.imdb.is_some() || ids.tmdb.is_some() || ids.tvdb_movie.is_some()
        }
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
        "SELECT imdb_id, anilist_id, tvdb_id FROM classifier_overrides WHERE library_type = ? AND normalized_key = ? LIMIT 1",
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

async fn persist_review_outcome(
    pool: &AnyPool,
    media_file_id: Uuid,
    outcome: &ReviewOutcome,
) -> Result<()> {
    match outcome.status {
        ReviewQueueStatus::Applied => mark_review_applied(pool, media_file_id).await?,
        ReviewQueueStatus::Pending => {
            upsert_review_queue_entry(
                pool,
                media_file_id,
                outcome,
            )
            .await?;
        }
    }
    Ok(())
}

async fn mark_review_applied(pool: &AnyPool, media_file_id: Uuid) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE review_queue SET status = 'applied', updated_at = CURRENT_TIMESTAMP WHERE media_file_id = ?",
    )
    .bind(media_file_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn upsert_review_queue_entry(
    pool: &AnyPool,
    media_file_id: Uuid,
    outcome: &ReviewOutcome,
) -> Result<()> {
    let existing = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT id FROM review_queue WHERE media_file_id = ? LIMIT 1",
    )
    .bind(media_file_id.to_string())
    .fetch_optional(pool)
    .await?;

    if let Some(id) = existing {
        sqlx::query::<sqlx::Any>(
            "UPDATE review_queue SET status = ?, confidence = ?, hint_json = ?, candidates_json = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        )
        .bind(outcome.status.as_str())
        .bind(outcome.confidence)
        .bind(outcome.hint_json.as_ref())
        .bind(outcome.candidates_json.as_ref())
        .bind(id)
        .execute(pool)
        .await?;
    } else {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO review_queue (id, media_file_id, status, confidence, hint_json, candidates_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(media_file_id.to_string())
        .bind(outcome.status.as_str())
        .bind(outcome.confidence)
        .bind(outcome.hint_json.as_ref())
        .bind(outcome.candidates_json.as_ref())
        .execute(pool)
        .await?;
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
            sqlx::query::<sqlx::Any>(
                "SELECT metadata_json, CAST(updated_at AS TEXT) as updated_at FROM movies WHERE title = ? AND (year IS ? OR year = ?) LIMIT 1",
            )
            .bind(&identity.title)
            .bind(identity.year)
            .bind(identity.year)
            .fetch_optional(pool)
            .await?
        }
        MediaType::Series | MediaType::Anime => {
            sqlx::query::<sqlx::Any>(
                "SELECT metadata_json, CAST(updated_at AS TEXT) as updated_at FROM series WHERE library_type = ? AND title = ? AND (year IS ? OR year = ?) LIMIT 1",
            )
            .bind(identity.r#type.as_str())
            .bind(&identity.title)
            .bind(identity.year)
            .bind(identity.year)
            .fetch_optional(pool)
            .await?
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

pub async fn run_extension_scan(state: &AppState, force_metadata: bool) -> Result<()> {
    let candidates = state
        .extensions
        .scan_all_with_db(&state.db_pool, &state.settings.library.sonarr, None)
        .await?;
    run_full_scan_with_metadata_and_linkers(
        &state.db_pool,
        Some(&state.metadata),
        Some(&state.linkers),
        Some(&state.settings.classifier),
        candidates,
        force_metadata,
        state.settings.library.hash_dedupe_enabled,
    )
    .await?;
    Ok(())
}

pub async fn start_periodic_scan(state: AppState, interval_seconds: u64) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_seconds));
    loop {
        interval.tick().await;
        if let Err(err) = run_extension_scan(&state, false).await {
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
    let existing = if let Some(imdb) = merged_ids.imdb.as_ref() {
        sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT id FROM movies WHERE external_imdb = ? LIMIT 1",
        )
        .bind(imdb)
        .fetch_optional(pool)
        .await?
    } else if let Some(tmdb) = merged_ids.tmdb.as_ref() {
        sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT id FROM movies WHERE external_tmdb = ? LIMIT 1",
        )
        .bind(tmdb)
        .fetch_optional(pool)
        .await?
    } else {
        sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT id FROM movies WHERE title = ? AND (year IS ? OR year = ?) LIMIT 1",
        )
        .bind(&identity.title)
        .bind(identity.year)
        .bind(identity.year)
        .fetch_optional(pool)
        .await?
    };

    if let Some(id_str) = existing {
        let id = Uuid::parse_str(&id_str)?;
        sqlx::query::<sqlx::Any>(
            "UPDATE movies SET title = ?, year = ?, external_imdb = ?, external_tmdb = ?, metadata_json = COALESCE(?, metadata_json), runtime_seconds = COALESCE(?, runtime_seconds), updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        )
        .bind(&identity.title)
        .bind(identity.year)
        .bind(merged_ids.imdb.as_ref())
        .bind(merged_ids.tmdb.as_ref())
        .bind(meta.and_then(|m| serde_json::to_string(&m.metadata_json).ok()))
        .bind(meta.and_then(|m| m.runtime_seconds))
        .bind(id_str)
        .execute(pool)
        .await?;
        return Ok(id);
    }

    let id = Uuid::new_v4();
    sqlx::query::<sqlx::Any>(
        "INSERT INTO movies (id, title, year, external_imdb, external_tmdb, metadata_json, runtime_seconds, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
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
    let existing = if let Some(anilist) = merged_ids.anilist.as_ref() {
        sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT id FROM series WHERE external_anilist = ? LIMIT 1",
        )
        .bind(anilist)
        .fetch_optional(pool)
        .await?
    } else if let Some(imdb) = merged_ids.imdb.as_ref() {
        sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT id FROM series WHERE external_imdb = ? LIMIT 1",
        )
        .bind(imdb)
        .fetch_optional(pool)
        .await?
    } else if let Some(tvdb) = merged_ids.tvdb_series.as_ref().or(merged_ids.tvdb.as_ref()) {
        sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT id FROM series WHERE external_tvdb_series = ? LIMIT 1",
        )
        .bind(tvdb)
        .fetch_optional(pool)
        .await?
    } else {
        sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT id FROM series WHERE library_type = ? AND title = ? AND (year IS ? OR year = ?) LIMIT 1",
        )
        .bind(identity.r#type.as_str())
        .bind(&identity.title)
        .bind(identity.year)
        .bind(identity.year)
        .fetch_optional(pool)
        .await?
    };

    if let Some(id_str) = existing {
        let id = Uuid::parse_str(&id_str)?;
        sqlx::query::<sqlx::Any>(
            "UPDATE series SET title = ?, year = ?, library_type = ?, external_imdb = ?, external_tvdb_series = ?, external_anilist = ?, metadata_json = COALESCE(?, metadata_json), updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        )
        .bind(&identity.title)
        .bind(identity.year)
        .bind(identity.r#type.as_str())
        .bind(merged_ids.imdb.as_ref())
        .bind(merged_ids.tvdb_series.as_ref().or(merged_ids.tvdb.as_ref()))
        .bind(merged_ids.anilist.as_ref())
        .bind(meta.and_then(|m| serde_json::to_string(&m.metadata_json).ok()))
        .bind(id_str)
        .execute(pool)
        .await?;
        return Ok(id);
    }

    let id = Uuid::new_v4();
    sqlx::query::<sqlx::Any>(
        "INSERT INTO series (id, title, year, library_type, external_imdb, external_tvdb_series, external_anilist, metadata_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
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

async fn upsert_season(pool: &AnyPool, series_id: Uuid, season_number: i32) -> Result<Uuid> {
    if let Some(id_str) = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT id FROM seasons WHERE series_id = ? AND season_number = ? LIMIT 1",
    )
    .bind(series_id.to_string())
    .bind(season_number)
    .fetch_optional(pool)
    .await?
    {
        let id = Uuid::parse_str(&id_str)?;
        sqlx::query::<sqlx::Any>(
            "UPDATE seasons SET updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        )
        .bind(id_str)
        .execute(pool)
        .await?;
        return Ok(id);
    }

    let id = Uuid::new_v4();
    sqlx::query::<sqlx::Any>(
        "INSERT INTO seasons (id, series_id, season_number, created_at, updated_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(id.to_string())
    .bind(series_id.to_string())
    .bind(season_number)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn upsert_episode(
    pool: &AnyPool,
    series_id: Uuid,
    season_id: Uuid,
    season_number: i32,
    episode_number: i32,
    absolute_episode_number: Option<i32>,
) -> Result<Uuid> {
    if let Some(id_str) = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT id FROM episodes WHERE series_id = ? AND season_number = ? AND episode_number = ? LIMIT 1",
    )
    .bind(series_id.to_string())
    .bind(season_number)
    .bind(episode_number)
    .fetch_optional(pool)
    .await?
    {
        let id = Uuid::parse_str(&id_str)?;
        sqlx::query::<sqlx::Any>(
            "UPDATE episodes SET absolute_episode_number = COALESCE(?, absolute_episode_number), updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        )
        .bind(absolute_episode_number)
        .bind(id_str)
        .execute(pool)
        .await?;
        return Ok(id);
    }

    let id = Uuid::new_v4();
    sqlx::query::<sqlx::Any>(
        "INSERT INTO episodes (id, series_id, season_id, season_number, episode_number, absolute_episode_number, has_file, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(id.to_string())
    .bind(series_id.to_string())
    .bind(season_id.to_string())
    .bind(season_number)
    .bind(episode_number)
    .bind(absolute_episode_number)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn upsert_legacy_media_item(
    pool: &AnyPool,
    id: Uuid,
    identity: &MediaIdentity,
    merged_ids: &ExternalIds,
    meta: Option<&MetadataResult>,
) -> Result<()> {
    let existing = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT id FROM media_items WHERE id = ? LIMIT 1",
    )
    .bind(id.to_string())
    .fetch_optional(pool)
    .await?;

    let external_ids_json = serde_json::to_string(merged_ids)?;
    if existing.is_some() {
        sqlx::query::<sqlx::Any>(
            "UPDATE media_items SET type = ?, title = ?, year = ?, external_ids = ?, metadata_json = COALESCE(?, metadata_json), runtime_seconds = COALESCE(?, runtime_seconds), updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        )
        .bind(identity.r#type.as_str())
        .bind(&identity.title)
        .bind(identity.year)
        .bind(external_ids_json)
        .bind(meta.and_then(|m| serde_json::to_string(&m.metadata_json).ok()))
        .bind(meta.and_then(|m| m.runtime_seconds))
        .bind(id.to_string())
        .execute(pool)
        .await?;
        return Ok(());
    }

    sqlx::query::<sqlx::Any>(
        "INSERT INTO media_items (id, type, external_ids, title, year, season, episode, metadata_json, runtime_seconds, created_at, updated_at) VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
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
    Ok(())
}

struct MediaFileUpsert {
    id: Uuid,
    duration_seconds: Option<i32>,
}

async fn upsert_media_file(
    pool: &AnyPool,
    legacy_item_id: Uuid,
    source_config_id: Option<Uuid>,
    file: &FileDescriptor,
    extension_metadata: Option<&HashMap<String, serde_json::Value>>,
    hash_dedupe: bool,
) -> Result<MediaFileUpsert> {
    let metadata = match ffprobe::probe(&file.path).await {
        Ok(metadata) => metadata,
        Err(err) => {
            tracing::warn!(path = %file.path, error = %err, "ffprobe failed during ingest");
            ffprobe::MediaMetadata::default()
        }
    };

    let mut existing = None;
    if hash_dedupe {
        if let Some(hash) = &file.hash {
            existing = sqlx::query::<sqlx::Any>(
                "SELECT id, source_config_id FROM media_files WHERE hash = ? LIMIT 1",
            )
            .bind(hash)
            .fetch_optional(pool)
            .await?;
        }
    }
    if existing.is_none() {
        existing = sqlx::query::<sqlx::Any>(
            "SELECT id, source_config_id FROM media_files WHERE path = ? LIMIT 1",
        )
        .bind(&file.path)
        .fetch_optional(pool)
        .await?;
    }

    if let Some(row) = existing {
        let id_str: String = row.get(0);
        let id = Uuid::parse_str(&id_str)?;
        let existing_source: Option<String> = row.try_get("source_config_id").ok();
        let desired_source = match (existing_source, source_config_id) {
            (None, Some(src)) => Some(src.to_string()),
            (Some(current), _) => Some(current),
            _ => None,
        };
        sqlx::query::<sqlx::Any>("UPDATE media_files SET media_item_id = ?, size_bytes = ?, container = ?, video_codec = ?, audio_codec = ?, width = COALESCE(?, width), height = COALESCE(?, height), bitrate_bps = COALESCE(?, bitrate_bps), hash = COALESCE(?, hash), extension_metadata = COALESCE(?, extension_metadata), updated_at = CURRENT_TIMESTAMP, scan_state = 'ok', source_config_id = COALESCE(source_config_id, ?) WHERE id = ?")
            .bind(legacy_item_id.to_string())
            .bind(file.size_bytes)
            .bind(metadata.container.as_ref().or(file.container.as_ref()))
            .bind(metadata.video_codec.as_ref().or(file.video_codec.as_ref()))
            .bind(metadata.audio_codec.as_ref().or(file.audio_codec.as_ref()))
            .bind(metadata.width)
            .bind(metadata.height)
            .bind(metadata.bitrate_bps)
            .bind(file.hash.as_ref())
            .bind(
                extension_metadata
                    .and_then(|m| serde_json::to_string(m).ok()),
            )
            .bind(desired_source)
            .bind(&id_str)
            .execute(pool)
            .await?;
        sync_media_tracks(pool, id, &metadata).await?;
        sync_external_subtitles(pool, id, &file.path).await?;
        return Ok(MediaFileUpsert {
            id,
            duration_seconds: metadata.duration_seconds,
        });
    }

    let id = Uuid::new_v4();
    sqlx::query::<sqlx::Any>("INSERT INTO media_files (id, media_item_id, source_config_id, path, size_bytes, container, video_codec, audio_codec, width, height, bitrate_bps, hash, extension_metadata, scan_state, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ok', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)")
        .bind(id.to_string())
        .bind(legacy_item_id.to_string())
        .bind(source_config_id.map(|u| u.to_string()))
        .bind(&file.path)
        .bind(file.size_bytes)
        .bind(metadata.container.as_ref().or(file.container.as_ref()))
        .bind(metadata.video_codec.as_ref().or(file.video_codec.as_ref()))
        .bind(metadata.audio_codec.as_ref().or(file.audio_codec.as_ref()))
        .bind(metadata.width)
        .bind(metadata.height)
        .bind(metadata.bitrate_bps)
        .bind(file.hash.as_ref())
        .bind(
            extension_metadata
                .and_then(|m| serde_json::to_string(m).ok()),
        )
        .execute(pool)
        .await?;
    sync_media_tracks(pool, id, &metadata).await?;
    sync_external_subtitles(pool, id, &file.path).await?;

    Ok(MediaFileUpsert {
        id,
        duration_seconds: metadata.duration_seconds,
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
}

async fn sync_media_tracks(
    pool: &AnyPool,
    media_file_id: Uuid,
    metadata: &ffprobe::MediaMetadata,
) -> Result<()> {
    if metadata.streams.is_empty() {
        return Ok(());
    }

    sqlx::query::<sqlx::Any>("DELETE FROM media_tracks WHERE media_file_id = ?")
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
        sqlx::query::<sqlx::Any>("INSERT INTO media_tracks (id, media_file_id, track_type, language, title, codec, channels, is_default, is_forced, stream_index, metadata_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)")
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

    sqlx::query::<sqlx::Any>("DELETE FROM external_subtitles WHERE media_file_id = ?")
        .bind(media_file_id.to_string())
        .execute(pool)
        .await?;

    for subtitle in subtitles {
        let id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>("INSERT INTO external_subtitles (id, media_file_id, path, language, title, format, is_default, is_forced, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)")
            .bind(id.to_string())
            .bind(media_file_id.to_string())
            .bind(subtitle.path)
            .bind(subtitle.language)
            .bind(subtitle.title)
            .bind(subtitle.format)
            .bind(subtitle.is_default)
            .bind(subtitle.is_forced)
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

fn parse_sidecar_attributes(tokens: &[String]) -> (Option<String>, Option<String>, bool, bool) {
    let mut language = None;
    let mut is_default = false;
    let mut is_forced = false;
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
                push_title_tag(&mut title_parts, "SDH");
            }
            "hearing_impaired" | "hearing-impaired" | "hearingimpaired" => {
                push_title_tag(&mut title_parts, "HI");
            }
            "hi" if language.is_some() => {
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

    (language, title, is_default, is_forced)
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
    matches!(ext, "srt" | "ass" | "ssa" | "vtt" | "sub" | "idx" | "sup" | "smi")
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
        let next_lower = tokens
            .get(idx + 1)
            .map(|v| v.to_ascii_lowercase());

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
    matches!(token, "x264" | "x265" | "h264" | "h265" | "hevc" | "av1" | "vp9")
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
        let (language, title, is_default, is_forced) = parse_sidecar_attributes(&tokens);
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
        "movie" => path.file_stem().and_then(|s| s.to_str()).map(|s| s.to_string()),
        "series" | "anime" => select_series_root_name(path),
        _ => path.file_stem().and_then(|s| s.to_str()).map(|s| s.to_string()),
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

async fn link_movie_file(pool: &AnyPool, movie_id: Uuid, media_file_id: Uuid) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "INSERT INTO movie_files (movie_id, media_file_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
    )
    .bind(movie_id.to_string())
    .bind(media_file_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn link_episode_file(pool: &AnyPool, episode_id: Uuid, media_file_id: Uuid) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "INSERT INTO episode_files (episode_id, media_file_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
    )
    .bind(episode_id.to_string())
    .bind(media_file_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn update_movie_runtime_if_missing(
    pool: &AnyPool,
    movie_id: Uuid,
    duration_seconds: i32,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE movies SET runtime_seconds = COALESCE(runtime_seconds, ?), updated_at = CURRENT_TIMESTAMP WHERE id = ?",
    )
    .bind(duration_seconds)
    .bind(movie_id.to_string())
    .execute(pool)
    .await?;

    let _ = sqlx::query::<sqlx::Any>(
        "UPDATE media_items SET runtime_seconds = COALESCE(runtime_seconds, ?), updated_at = CURRENT_TIMESTAMP WHERE id = ?",
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
        "UPDATE episodes SET runtime_seconds = COALESCE(runtime_seconds, ?), updated_at = CURRENT_TIMESTAMP WHERE id = ?",
    )
    .bind(duration_seconds)
    .bind(episode_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn mark_episode_has_file(pool: &AnyPool, episode_id: Uuid) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE episodes SET has_file = 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
    )
    .bind(episode_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn refresh_episode_file_state(pool: &AnyPool) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE episodes SET has_file = CASE WHEN EXISTS (SELECT 1 FROM episode_files ef JOIN media_files mf ON mf.id = ef.media_file_id WHERE ef.episode_id = episodes.id AND mf.scan_state = 'ok') THEN 1 ELSE 0 END",
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
        "UPDATE movies SET external_imdb = COALESCE(?, external_imdb), external_tmdb = COALESCE(?, external_tmdb), updated_at = CURRENT_TIMESTAMP WHERE id = ?",
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
    let tvdb = ids
        .tvdb_series
        .as_ref()
        .or(ids.tvdb.as_ref())
        .cloned();
    sqlx::query::<sqlx::Any>(
        "UPDATE series SET external_imdb = COALESCE(?, external_imdb), external_tvdb_series = COALESCE(?, external_tvdb_series), external_anilist = COALESCE(?, external_anilist), updated_at = CURRENT_TIMESTAMP WHERE id = ?",
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

    for (provider, external_id) in entries {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO movie_external_ids (id, movie_id, provider, external_id, confidence, source) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
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

    Ok(())
}

async fn persist_series_external_ids(
    pool: &AnyPool,
    series_id: Uuid,
    ids: &ExternalIds,
    source: &str,
) -> Result<()> {
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

    for (provider, external_id) in entries {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO series_external_ids (id, series_id, provider, external_id, confidence, source) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(series_id.to_string())
        .bind(provider)
        .bind(external_id)
        .bind(1.0_f32)
        .bind(source)
        .execute(pool)
        .await?;
    }

    Ok(())
}

async fn ensure_tvdb_season_scaffold(
    pool: &AnyPool,
    series_id: Uuid,
    season_id: Uuid,
    tvdb_series_id: &str,
    season_number: i32,
    linker: &LinkerService,
) -> Result<()> {
    if season_scaffolded(pool, season_id).await? {
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
        if let Some(tvdb_episode_id) = episode.tvdb_episode_id.as_ref() {
            insert_episode_external_id(
                pool,
                episode_id,
                "tvdb_episode",
                tvdb_episode_id,
                "tvdb",
            )
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
) -> Result<()> {
    if season_scaffolded(pool, season_id).await? {
        return Ok(());
    }

    let mut processed = 0usize;
    for episode in mapping.episodes.iter().filter(|ep| {
        ep.season_number
            .map(|num| num == season_number)
            .unwrap_or(false)
    }) {
        let ep_number = match episode.episode_number {
            Some(num) => num,
            None => continue,
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
        upsert_anime_episode_meta(
            pool,
            season_id,
            ep_number,
            episode,
        )
        .await?;
        if let Some(tvdb_id) = episode.tvdb_id.as_ref() {
            insert_episode_external_id(
                pool,
                episode_id,
                "tvdb_episode",
                tvdb_id,
                "anizip",
            )
            .await?;
            insert_episode_provider_key(pool, episode_id, "tvdb", tvdb_id).await?;
        }
        if let Some(anidb_eid) = episode.anidb_eid.as_ref() {
            insert_episode_external_id(
                pool,
                episode_id,
                "anidb_episode",
                anidb_eid,
                "anizip",
            )
            .await?;
            insert_episode_provider_key(pool, episode_id, "anidb", anidb_eid).await?;
        }
        processed += 1;
    }

    if processed > 0 {
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
        "UPDATE episodes SET title = COALESCE(?, title), runtime_seconds = COALESCE(?, runtime_seconds), metadata_json = COALESCE(?, metadata_json), updated_at = CURRENT_TIMESTAMP WHERE id = ?",
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
        "INSERT INTO episode_external_ids (id, episode_id, provider, external_id, confidence, source) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
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
    let existing = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT id FROM anime_episode_meta WHERE season_id = ? AND episode_number = ? LIMIT 1",
    )
    .bind(season_id.to_string())
    .bind(episode_number)
    .fetch_optional(pool)
    .await?;

    if let Some(id_str) = existing {
        sqlx::query::<sqlx::Any>(
            "UPDATE anime_episode_meta SET title = COALESCE(?, title), snapshot_url = COALESCE(?, snapshot_url), duration_seconds = COALESCE(?, duration_seconds), raw_json = COALESCE(?, raw_json), updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        )
        .bind(episode.title.as_deref())
        .bind(episode.image.as_deref())
        .bind(duration_seconds)
        .bind(raw_json)
        .bind(id_str)
        .execute(pool)
        .await?;
        return Ok(());
    }

    let id = Uuid::new_v4();
    sqlx::query::<sqlx::Any>(
        "INSERT INTO anime_episode_meta (id, season_id, episode_number, title, snapshot_url, duration_seconds, raw_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(id.to_string())
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
        "INSERT INTO episode_provider_keys (id, episode_id, provider, provider_key) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(episode_id.to_string())
    .bind(provider)
    .bind(provider_key)
    .execute(pool)
    .await?;
    Ok(())
}

async fn season_scaffolded(pool: &AnyPool, season_id: Uuid) -> Result<bool> {
    let meta: Option<String> = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT metadata_json FROM seasons WHERE id = ? LIMIT 1",
    )
    .bind(season_id.to_string())
    .fetch_optional(pool)
    .await?;
    let Some(meta) = meta else {
        return Ok(false);
    };
    let parsed: serde_json::Value = serde_json::from_str(&meta).unwrap_or_default();
    Ok(parsed
        .get("scaffolded")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false))
}

async fn mark_season_scaffolded(
    pool: &AnyPool,
    season_id: Uuid,
    provider: &str,
) -> Result<()> {
    let existing: Option<String> = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT metadata_json FROM seasons WHERE id = ? LIMIT 1",
    )
    .bind(season_id.to_string())
    .fetch_optional(pool)
    .await?;

    let mut meta = existing
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .unwrap_or_else(|| serde_json::json!({}));

    if let Some(obj) = meta.as_object_mut() {
        obj.insert("scaffolded".to_string(), serde_json::json!(true));
        obj.insert(
            "scaffold_provider".to_string(),
            serde_json::json!(provider),
        );
        obj.insert(
            "scaffolded_at".to_string(),
            serde_json::json!(Utc::now().to_rfc3339()),
        );
    }

    sqlx::query::<sqlx::Any>(
        "UPDATE seasons SET metadata_json = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::DatabaseConfig,
        db::Database,
        extensions::{ExternalIds as ExtIds, FileDescriptor as FD, MediaIdentity},
    };
    use std::collections::HashMap;

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

        let (movie_count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM movies")
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(movie_count, 1);

        let (link_count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM movie_files")
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(link_count, 1);

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
    async fn classifier_creates_review_queue_when_ids_missing() -> Result<()> {
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

        let (queue_count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM review_queue")
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(queue_count, 1);

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
        let normalized =
            derive_override_key("movie", media_path).expect("override key");

        sqlx::query(
            "INSERT INTO classifier_overrides (id, library_type, normalized_key, imdb_id, anilist_id, tvdb_id) VALUES (?, ?, ?, ?, NULL, NULL)",
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

        let imdb: Option<String> = sqlx::query_scalar(
            "SELECT external_imdb FROM movies LIMIT 1",
        )
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
}
