mod linkers;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
use elixir_classifier::identify::anilist::AniListIdentifier;
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
use uuid::Uuid;

use crate::{
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
    state::AppState,
};

pub use linkers::{AniZipEpisodeRecord, AniZipMapping, LinkerService};

const ANILIST_ENDPOINT: &str = "https://graphql.anilist.co";

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
    upsert_legacy_media_item(pool, movie_id, &identity, &merged_ids, meta.as_ref()).await?;

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
    link_movie_file(pool, movie_id, media_file.id).await?;
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
    upsert_legacy_media_item(pool, series_id, &identity, &series_ids, meta.as_ref()).await?;
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
        link_episode_file(pool, episode_id, media_file.id).await?;
        mark_episode_has_file(pool, episode_id).await?;
        if let Some(duration) = media_file.duration_seconds {
            update_episode_runtime_if_missing(pool, episode_id, duration).await?;
        }
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
    let classifier = build_classifier_pipeline(classifier_config);
    let anilist_bridge = build_anilist_identifier(classifier_config);
    let anilist_scorer = DefaultScorer::default();
    let hydration_ttl_seconds = metadata.map(|service| service.ttl_seconds()).unwrap_or(0);

    for mut candidate in merged {
        let mut merged_ids = candidate.identity.external_ids.clone();
        if let Some(identity_lock) =
            load_managed_identity_lock_for_files(pool, &candidate.files).await?
        {
            apply_managed_identity_lock(&mut candidate.identity, &mut merged_ids, identity_lock);
        }
        let mut matched_intent = match_and_merge_managed_ingest_intent(
            &mut candidate,
            &mut merged_ids,
            &managed_ingest_intents,
            &mut matched_managed_intent_ids,
        );
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
        let (
            classified_ids,
            mut review_outcomes,
            mut prefer_anime,
            tvdb_seeds,
            mut season_anilist_seeds,
        ) = classify_candidate_files(pool, &classifier, &candidate, &merged_ids, force_reclassify)
            .await?;
        merged_ids = classified_ids;
        if matched_intent.is_none() {
            matched_intent = match_and_merge_managed_ingest_intent(
                &mut candidate,
                &mut merged_ids,
                &managed_ingest_intents,
                &mut matched_managed_intent_ids,
            );
        }

        let mut anizip_mappings: HashMap<i32, AniZipMapping> = HashMap::new();
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
                    if let Ok(Some(series_meta)) = linker.fetch_tvdb_series(&tvdb_id).await {
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
                                    .partial_cmp(&a.confidence)
                                    .unwrap_or(std::cmp::Ordering::Equal)
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
                                    &mut review_outcomes,
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

        let mut expanded_chain: Vec<AniListSeasonChainEntry> = Vec::new();
        if !season_anilist_seeds.is_empty() {
            if let Some((seed_season, seed)) = season_anilist_seeds.iter().max_by(|a, b| {
                a.1.confidence
                    .partial_cmp(&b.1.confidence)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }) {
                let expanded =
                    expand_anilist_season_chain(&anilist_bridge, *seed_season, seed).await?;
                if !expanded.is_empty() {
                    tracing::trace!(
                        seed_season = seed_season,
                        expanded = expanded.len(),
                        "expanded anilist season chain"
                    );
                    expanded_chain = expanded.clone();
                    for entry in expanded {
                        insert_season_anilist_seed(
                            &mut season_anilist_seeds,
                            entry.season_number,
                            SeasonAnilistSeed {
                                anilist_id: entry.anilist_id,
                                confidence: entry.confidence,
                            },
                        );
                    }
                }
            }
        }

        if prefer_anime && candidate.identity.r#type != MediaType::Anime {
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
                &mut matched_managed_intent_ids,
            );
        }

        let has_anime_ids = merged_ids.anilist.is_some()
            || merged_ids.anidb.is_some()
            || merged_ids.mal.is_some()
            || merged_ids.kitsu.is_some();
        if has_anime_ids && candidate.identity.r#type != MediaType::Anime {
            candidate.identity.r#type = MediaType::Anime;
        }

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
                let mut series_ids = if candidate.identity.r#type == MediaType::Anime {
                    strip_anime_ids(&merged_ids)
                } else {
                    merged_ids.clone()
                };
                let series_id =
                    upsert_series(pool, &candidate.identity, &series_ids, meta.as_ref()).await?;
                upsert_legacy_media_item(
                    pool,
                    series_id,
                    &candidate.identity,
                    &series_ids,
                    meta.as_ref(),
                )
                .await?;
                persist_series_external_ids(pool, series_id, &series_ids, "classifier").await?;

                let mut resolved_numbers: HashMap<String, ResolvedEpisodeNumbers> = HashMap::new();
                for file in &candidate.files {
                    let outcome = review_outcomes.get(&file.descriptor.path);
                    let resolved = resolve_episode_numbers(
                        file,
                        outcome,
                        candidate.identity.r#type,
                        &anizip_mappings,
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

                if candidate.identity.r#type == MediaType::Anime && !season_anilist_seeds.is_empty()
                {
                    for season_number in season_anilist_seeds.keys() {
                        if season_ids.contains_key(season_number) {
                            continue;
                        }
                        let season_id = upsert_season(pool, series_id, *season_number).await?;
                        season_ids.insert(*season_number, season_id);
                    }
                }

                if candidate.identity.r#type == MediaType::Series {
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

                for (season_number, season_id) in &season_ids {
                    if !season_anilist_seeds.contains_key(season_number) {
                        if let Some(raw) = sqlx::query_scalar::<sqlx::Any, String>(
                            "SELECT COALESCE(external_anilist, '') FROM seasons WHERE id = ? LIMIT 1",
                        )
                        .bind(season_id.to_string())
                        .fetch_optional(pool)
                        .await?
                        {
                            let trimmed = raw.trim();
                            if !trimmed.is_empty() {
                                tracing::trace!(
                                    season = season_number,
                                    anilist_id = %trimmed,
                                    "loaded season anilist id from db"
                                );
                                insert_season_anilist_seed(
                                    &mut season_anilist_seeds,
                                    *season_number,
                                    SeasonAnilistSeed {
                                        anilist_id: trimmed.to_string(),
                                        confidence: 0.5,
                                    },
                                );
                            }
                        }
                    }
                }

                if candidate.identity.r#type == MediaType::Anime
                    || prefer_anime
                    || merged_ids.anilist.is_some()
                    || !season_anilist_seeds.is_empty()
                {
                    mark_series_as_anime(pool, series_id).await?;
                }

                if let Some(linker) = linkers {
                    for (season_number, seed) in &season_anilist_seeds {
                        if anizip_mappings.contains_key(season_number) {
                            continue;
                        }
                        if let Some(season_id) = season_ids.get(season_number) {
                            let is_fresh = season_scaffolded_recent(
                                pool,
                                *season_id,
                                Some("anizip"),
                                hydration_ttl_seconds,
                                force_metadata,
                            )
                            .await?;
                            if is_fresh {
                                continue;
                            }
                        }
                        if let Ok(Some(mapping)) =
                            linker.fetch_anizip_mapping(&seed.anilist_id).await
                        {
                            merged_ids = merge_external_ids(&merged_ids, Some(mapping.ids.clone()));
                            anizip_mappings.insert(*season_number, mapping);
                        }
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
                    if let Some(seed) = season_anilist_seeds.get(season_number) {
                        let ids = ExternalIds {
                            anilist: Some(seed.anilist_id.clone()),
                            ..Default::default()
                        };
                        apply_external_ids_to_season(
                            pool,
                            *season_id,
                            &ids,
                            "classifier",
                            Some(seed.confidence),
                        )
                        .await?;
                        persist_series_external_ids(pool, series_id, &ids, "anilist_chain").await?;
                    }
                    if let Some(mapping) = anizip_mappings.get(season_number) {
                        apply_external_ids_to_season(
                            pool,
                            *season_id,
                            &mapping.ids,
                            "anizip",
                            None,
                        )
                        .await?;
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

                if let Some(linker) = linkers {
                    for (season_number, season_id) in &season_ids {
                        if season_scaffolded_recent(
                            pool,
                            *season_id,
                            None,
                            hydration_ttl_seconds,
                            force_metadata,
                        )
                        .await?
                        {
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
                        } else if let Some(tvdb_id) = merged_ids.tvdb_series.as_ref() {
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
                }

                for file in candidate.files {
                    let resolved = resolved_numbers
                        .get(&file.descriptor.path)
                        .copied()
                        .unwrap_or(ResolvedEpisodeNumbers {
                            season: file.season,
                            episode: file.episode,
                            absolute_episode: file.absolute_episode,
                        });
                    if let Some(tombstone) = match_managed_episode_tombstone(
                        &candidate.identity,
                        &merged_ids,
                        resolved.season.unwrap_or(1),
                        resolved.episode.unwrap_or(1),
                        resolved.absolute_episode,
                        &managed_episode_tombstones,
                    ) {
                        tracing::info!(
                            title = %candidate.identity.title,
                            media_type = %candidate.identity.r#type.as_str(),
                            season = resolved.season.unwrap_or(1),
                            episode = resolved.episode.unwrap_or(1),
                            tombstone_id = %tombstone.tombstone_id,
                            "skipping managed episode candidate because it is blocked by an episode tombstone"
                        );
                        seen_paths.insert(file.descriptor.path);
                        continue;
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
                    if let Some(outcome) = review_outcomes.get(&file.descriptor.path) {
                        persist_review_outcome(pool, media_file.id, outcome).await?;
                    }
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

    for intent_id in matched_managed_intent_ids {
        extension_store
            .mark_managed_ingest_intent_matched(intent_id)
            .await?;
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
    matched_intent_ids: &mut HashSet<Uuid>,
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
    matched_intent_ids.insert(intent.intent_id);
    Some(intent)
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
             WHERE mf.path = ?
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
}

#[derive(Debug, Clone)]
struct ExistingFileClassification {
    ids: ExternalIds,
    prefer_anime: bool,
    media_file_id: String,
}

async fn load_existing_classification_for_path(
    pool: &AnyPool,
    path: &str,
    expected_type: MediaType,
) -> Result<Option<ExistingFileClassification>> {
    let media_file_id: Option<String> = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT id FROM media_files WHERE path = ? LIMIT 1",
    )
    .bind(path)
    .fetch_optional(pool)
    .await?;
    let Some(media_file_id) = media_file_id else {
        return Ok(None);
    };

    let mut movie_id: Option<String> = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT movie_id FROM movie_files WHERE media_file_id = ? LIMIT 1",
    )
    .bind(&media_file_id)
    .fetch_optional(pool)
    .await?;

    let mut episode_row = sqlx::query(
        "SELECT e.series_id as series_id, e.season_id as season_id \
         FROM episode_files ef JOIN episodes e ON e.id = ef.episode_id \
         WHERE ef.media_file_id = ? LIMIT 1",
    )
    .bind(&media_file_id)
    .fetch_optional(pool)
    .await?;

    let has_review: Option<i64> = sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT 1 FROM review_queue WHERE media_file_id = ? LIMIT 1",
    )
    .bind(&media_file_id)
    .fetch_optional(pool)
    .await?;

    if movie_id.is_some() && episode_row.is_some() {
        tracing::warn!(
            media_file_id = %media_file_id,
            expected_type = ?expected_type,
            "media file linked to both movie and episode; cleaning up stale link"
        );
        match expected_type {
            MediaType::Movie => {
                unlink_episode_links_for_media_file(pool, &media_file_id).await?;
                episode_row = None;
            }
            MediaType::Series | MediaType::Anime => {
                unlink_movie_links_for_media_file(pool, &media_file_id).await?;
                movie_id = None;
            }
        }
    }

    if movie_id.is_none() && episode_row.is_none() && has_review.is_none() {
        return Ok(None);
    }

    let mut ids = ExternalIds::default();

    if let Some(movie_id) = movie_id {
        if let Some(row) =
            sqlx::query("SELECT external_imdb, external_tmdb FROM movies WHERE id = ? LIMIT 1")
                .bind(&movie_id)
                .fetch_optional(pool)
                .await?
        {
            ids.imdb = row.try_get::<String, _>("external_imdb").ok();
            ids.tmdb = row.try_get::<String, _>("external_tmdb").ok();
        }
    }

    if let Some(row) = episode_row {
        let series_id: String = row.get("series_id");
        let season_id: String = row.get("season_id");

        if let Some(series_row) = sqlx::query(
            "SELECT external_imdb, external_tvdb_series, external_anilist \
             FROM series WHERE id = ? LIMIT 1",
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
        }

        if let Some(season_row) =
            sqlx::query("SELECT external_anilist FROM seasons WHERE id = ? LIMIT 1")
                .bind(&season_id)
                .fetch_optional(pool)
                .await?
        {
            if ids.anilist.is_none() {
                ids.anilist = season_row.try_get::<String, _>("external_anilist").ok();
            }
        }
    }

    let prefer_anime =
        ids.anilist.is_some() || ids.anidb.is_some() || ids.mal.is_some() || ids.kitsu.is_some();

    Ok(Some(ExistingFileClassification {
        ids,
        prefer_anime,
        media_file_id,
    }))
}

#[derive(Debug, Clone)]
pub struct AniListSeasonChainEntry {
    pub season_number: i32,
    pub anilist_id: String,
    pub confidence: f32,
}

async fn classify_candidate_files(
    pool: &AnyPool,
    classifier: &ClassifierPipeline,
    candidate: &AggregatedCandidate,
    merged_ids: &ExternalIds,
    force_reclassify: bool,
) -> Result<(
    ExternalIds,
    HashMap<String, ReviewOutcome>,
    bool,
    HashMap<i32, TvdbBridgeSeed>,
    HashMap<i32, SeasonAnilistSeed>,
)> {
    let library_type = candidate.identity.r#type;
    let library_type_key = library_type_string(library_type);
    let mut updated_ids = merged_ids.clone();
    let mut outcomes: HashMap<String, ReviewOutcome> = HashMap::new();
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
        if let Some(override_ids) =
            lookup_override_for_path(pool, library_type_key, path, &mut override_cache).await?
        {
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

        if let Some(existing) =
            load_existing_classification_for_path(pool, path, effective_type).await?
        {
            if !force_reclassify {
                updated_ids = merge_external_ids(&updated_ids, Some(existing.ids));
                if existing.prefer_anime {
                    prefer_anime = true;
                }
                tracing::trace!(
                    path = %path,
                    media_file_id = %existing.media_file_id,
                    "classifier skipping existing file"
                );
                continue;
            }
        }

        if has_strong_ids(effective_type, &updated_ids) {
            tracing::trace!(
                path = %path,
                effective_type = ?effective_type,
                ids = ?updated_ids,
                "classifier strong ids present; skipping identify"
            );
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

        let input = build_classifier_input(file, effective_type, &updated_ids);
        let results = classifier.classify_file(&input).await?;
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

        let outcome = match selection {
            Some((hint, canonical)) => {
                let decision = review_decision_from_match(canonical.as_ref());
                let review_recommended = canonical
                    .as_ref()
                    .map(|c| c.confidence >= 0.65 && c.confidence < 0.85)
                    .unwrap_or(false);
                tracing::trace!(
                    path = %path,
                    hint_type = ?hint.library_type,
                    hint_title = %hint.title,
                    chosen_provider = canonical.as_ref().map(|c| c.chosen_provider),
                    confidence = canonical.as_ref().map(|c| c.confidence),
                    decision = %decision.as_str(),
                    review_recommended,
                    "classifier selected hint"
                );
                let (hint_json, candidates_json) =
                    build_review_payloads(&hint, canonical.as_ref(), review_recommended)?;
                if let Some(canonical) = canonical.as_ref() {
                    let season_number = hint.season.or(file.season).unwrap_or(1);
                    if canonical.chosen_provider == "tvdb" {
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
                    if let Some(anilist_id) = canonical.ids.anilist.as_ref() {
                        let seed = SeasonAnilistSeed {
                            anilist_id: anilist_id.clone(),
                            confidence: canonical.confidence,
                        };
                        let replace = anilist_seeds
                            .get(&season_number)
                            .map(|current| seed.confidence > current.confidence)
                            .unwrap_or(true);
                        if replace {
                            anilist_seeds.insert(season_number, seed);
                            tracing::trace!(
                                path = %path,
                                season = season_number,
                                anilist_id = %anilist_id,
                                confidence = canonical.confidence,
                                "classifier stored anilist season seed"
                            );
                        }
                    }
                    let mapped = classifier_ids_to_server(&canonical.ids, effective_type);
                    updated_ids = merge_external_ids(&updated_ids, Some(mapped));
                    let before_prefer = prefer_anime;
                    if canonical.ids.anilist.is_some()
                        || canonical.ids.anidb.is_some()
                        || canonical.ids.mal.is_some()
                        || canonical.ids.kitsu.is_some()
                    {
                        prefer_anime = true;
                    }
                    if prefer_anime != before_prefer {
                        tracing::trace!(
                            path = %path,
                            prefer_anime,
                            "classifier prefer_anime enabled from candidate ids"
                        );
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

fn resolve_episode_numbers(
    file: &AggregatedFile,
    outcome: Option<&ReviewOutcome>,
    media_type: MediaType,
    anizip_mappings: &HashMap<i32, AniZipMapping>,
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
        if season.is_none() || episode.is_none() {
            if let Some(abs) = absolute_episode {
                let mapped = if let Some(current_season) = season {
                    anizip_mappings
                        .get(&current_season)
                        .and_then(|mapping| lookup_anizip_absolute_episode(Some(mapping), abs))
                } else {
                    None
                }
                .or_else(|| lookup_anizip_absolute_episode_from_maps(anizip_mappings, abs));
                if let Some((mapped_season, mapped_episode)) = mapped {
                    tracing::trace!(
                        path = %file.descriptor.path,
                        absolute_episode = ?absolute_episode,
                        mapped_season,
                        mapped_episode,
                        "anizip absolute episode mapped"
                    );
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

fn lookup_anizip_absolute_episode_from_maps(
    mappings: &HashMap<i32, AniZipMapping>,
    absolute_episode: i32,
) -> Option<(i32, i32)> {
    for mapping in mappings.values() {
        if let Some(found) = lookup_anizip_absolute_episode(Some(mapping), absolute_episode) {
            return Some(found);
        }
    }
    None
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
            obj.insert(
                "reviewRecommended".to_string(),
                serde_json::Value::Bool(true),
            );
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

fn insert_season_anilist_seed(
    seeds: &mut HashMap<i32, SeasonAnilistSeed>,
    season_number: i32,
    seed: SeasonAnilistSeed,
) {
    let replace = seeds
        .get(&season_number)
        .map(|current| seed.confidence > current.confidence)
        .unwrap_or(true);
    if replace {
        tracing::trace!(
            season = season_number,
            anilist_id = %seed.anilist_id,
            confidence = seed.confidence,
            "season anilist seed updated"
        );
        seeds.insert(season_number, seed);
    } else {
        tracing::trace!(
            season = season_number,
            anilist_id = %seed.anilist_id,
            confidence = seed.confidence,
            "season anilist seed retained"
        );
    }
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
        return Ok(Vec::new());
    }

    let seed_index = match filtered.iter().position(|node| node.id == seed_id) {
        Some(idx) => idx,
        None => {
            tracing::warn!(
                anilist_id = %seed.anilist_id,
                "anilist seed id not found in relation chain"
            );
            return Ok(Vec::new());
        }
    };

    let mut expanded = Vec::new();
    for (idx, node) in filtered.iter().enumerate() {
        let offset = idx as i32 - seed_index as i32;
        let season_number = seed_season + offset;
        if season_number < 1 {
            continue;
        }
        let confidence = if node.id == seed_id {
            seed.confidence
        } else {
            seed.confidence * 0.8
        };
        expanded.push(AniListSeasonChainEntry {
            season_number,
            anilist_id: node.id.to_string(),
            confidence,
        });
    }

    if !expanded.is_empty() {
        tracing::trace!(
            seed_anilist_id = %seed.anilist_id,
            seasons = expanded.len(),
            "anilist season chain resolved"
        );
    }

    Ok(expanded)
}

pub async fn resolve_anilist_season_chain(
    config: Option<&ClassifierConfig>,
    seed_season: i32,
    anilist_id: &str,
    confidence: f32,
) -> Result<Vec<AniListSeasonChainEntry>> {
    let seed = SeasonAnilistSeed {
        anilist_id: anilist_id.trim().to_string(),
        confidence,
    };
    if seed.anilist_id.is_empty() || seed_season < 1 {
        return Ok(Vec::new());
    }
    let anilist = build_anilist_identifier(config);
    expand_anilist_season_chain(&anilist, seed_season, &seed).await
}

fn build_anilist_identifier(config: Option<&ClassifierConfig>) -> AniListIdentifier {
    let timeout = config.map(|cfg| cfg.request_timeout_seconds).unwrap_or(10);
    AniListIdentifier::new(ANILIST_ENDPOINT.to_string(), timeout)
}

async fn apply_tvdb_anime_bridge(
    series_meta: &serde_json::Value,
    anilist: &AniListIdentifier,
    scorer: &DefaultScorer,
    merged_ids: &mut ExternalIds,
    review_outcomes: &mut HashMap<String, ReviewOutcome>,
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
            return Ok(result);
        }
    };
    let mut scored_candidates = candidates;
    if let Some(season) = hint.season {
        let filtered: Vec<_> = scored_candidates
            .iter()
            .cloned()
            .filter(|candidate| candidate.season == Some(season))
            .collect();
        if !filtered.is_empty() {
            tracing::trace!(
                tvdb_id = %tvdb_id,
                season,
                before = scored_candidates.len(),
                after = filtered.len(),
                "tvdb anime bridge filtered candidates by season"
            );
            scored_candidates = filtered;
        }
    }
    let mut canonical = scorer.score(&hint, &scored_candidates);

    if canonical
        .as_ref()
        .map(|value| value.confidence < 0.65)
        .unwrap_or(true)
    {
        tracing::trace!(
            tvdb_id = %tvdb_id,
            season = seed.season_number,
            confidence = canonical.as_ref().map(|value| value.confidence),
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
            let mut alias_scored = alias_candidates;
            if let Some(season) = hint.season {
                let filtered: Vec<_> = alias_scored
                    .iter()
                    .cloned()
                    .filter(|candidate| candidate.season == Some(season))
                    .collect();
                if !filtered.is_empty() {
                    tracing::trace!(
                        tvdb_id = %tvdb_id,
                        season,
                        before = alias_scored.len(),
                        after = filtered.len(),
                        "tvdb anime bridge filtered alias candidates by season"
                    );
                    alias_scored = filtered;
                }
            }
            let alias_canonical = scorer.score(&hint, &alias_scored);
            let alias_confidence = alias_canonical
                .as_ref()
                .map(|c| c.confidence)
                .unwrap_or(0.0);
            let current_confidence = canonical.as_ref().map(|c| c.confidence).unwrap_or(0.0);
            if alias_confidence > current_confidence {
                canonical = alias_canonical;
            }
        }
    }
    if let Some(canonical) = canonical.as_ref() {
        tracing::debug!(
            tvdb_id = %tvdb_id,
            anilist_id = ?canonical.ids.anilist,
            confidence = canonical.confidence,
            "anilist bridge result"
        );
    } else {
        tracing::debug!(tvdb_id = %tvdb_id, "anilist bridge produced no candidates");
    }

    if let Some(canonical) = canonical.as_ref() {
        let meets_season_bridge_threshold = canonical.considered.first().map_or(false, |best| {
            canonical.confidence >= 0.55
                && best.features.title_similarity >= 0.5
                && best.features.season_match >= 0.99
        });
        if canonical.confidence >= 0.65 || meets_season_bridge_threshold {
            if let Some(anilist_id) = canonical.ids.anilist.as_ref() {
                let season_seed = SeasonAnilistSeed {
                    anilist_id: anilist_id.clone(),
                    confidence: canonical.confidence,
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

    let decision = review_decision_from_match(canonical.as_ref());
    let review_recommended = canonical
        .as_ref()
        .map(|c| c.confidence >= 0.65 && c.confidence < 0.85)
        .unwrap_or(false);
    let (hint_json, candidates_json) =
        build_review_payloads(&hint, canonical.as_ref(), review_recommended)?;

    for outcome in review_outcomes.values_mut() {
        let season = outcome
            .parsed_hint
            .as_ref()
            .and_then(|hint| hint.season)
            .unwrap_or(seed.season_number);
        if season != seed.season_number {
            continue;
        }
        outcome.status = decision;
        outcome.confidence = canonical.as_ref().map(|c| c.confidence);
        outcome.hint_json = hint_json.clone();
        outcome.candidates_json = candidates_json.clone();
    }

    Ok(result)
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
        let key = trimmed.to_ascii_lowercase();
        if seen.insert(key) {
            out.push(trimmed.to_string());
        }
    }
    out
}

fn select_best_classification(
    results: Vec<ClassifiedHint>,
) -> Option<(
    elixir_classifier::hint::ClassificationHint,
    Option<ClassifierCanonicalMatch>,
)> {
    let mut best: Option<(
        elixir_classifier::hint::ClassificationHint,
        Option<ClassifierCanonicalMatch>,
    )> = None;
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
    if let Some((hint, canonical)) = best.as_ref() {
        tracing::trace!(
            hint_type = ?hint.library_type,
            hint_title = %hint.title,
            confidence = canonical.as_ref().map(|c| c.confidence),
            chosen_provider = canonical.as_ref().map(|c| c.chosen_provider),
            "classifier selected best hint"
        );
    } else {
        tracing::trace!("classifier selected no hint");
    }
    best
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
            upsert_review_queue_entry(pool, media_file_id, outcome).await?;
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
            let mut existing = None;
            if let Some(imdb) = identity.external_ids.imdb.as_ref() {
                existing = sqlx::query::<sqlx::Any>(
                    "SELECT metadata_json, CAST(updated_at AS TEXT) as updated_at FROM movies WHERE external_imdb = ? LIMIT 1",
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
                     WHERE mei.provider = 'tvdb' AND mei.external_id = ?
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
                    "SELECT metadata_json, CAST(updated_at AS TEXT) as updated_at FROM movies WHERE external_tmdb = ? LIMIT 1",
                )
                .bind(tmdb)
                .fetch_optional(pool)
                    .await?;
                }
            }
            if existing.is_none() {
                existing = sqlx::query::<sqlx::Any>(
                    "SELECT metadata_json, CAST(updated_at AS TEXT) as updated_at FROM movies WHERE title = ? AND (year IS ? OR year = ?) LIMIT 1",
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
                    "SELECT metadata_json, CAST(updated_at AS TEXT) as updated_at FROM series WHERE library_type = ? AND external_anilist = ? LIMIT 1",
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
                    "SELECT metadata_json, CAST(updated_at AS TEXT) as updated_at FROM series WHERE library_type = ? AND external_tvdb_series = ? LIMIT 1",
                )
                .bind(library_type)
                .bind(tvdb)
                .fetch_optional(pool)
                .await?
            } else if let Some(imdb) = identity.external_ids.imdb.as_ref() {
                sqlx::query::<sqlx::Any>(
                    "SELECT metadata_json, CAST(updated_at AS TEXT) as updated_at FROM series WHERE library_type = ? AND external_imdb = ? LIMIT 1",
                )
                .bind(library_type)
                .bind(imdb)
                .fetch_optional(pool)
                .await?
            } else {
                sqlx::query::<sqlx::Any>(
                    "SELECT metadata_json, CAST(updated_at AS TEXT) as updated_at FROM series WHERE library_type = ? AND title = ? AND (year IS ? OR year = ?) LIMIT 1",
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
    run_full_scan_with_metadata_and_linkers(
        &state.db_pool,
        Some(&state.metadata),
        Some(&state.linkers),
        Some(&state.settings.classifier),
        Some(&state.artwork),
        candidates,
        force_metadata,
        force_reclassify,
        state.settings.library.hash_dedupe_enabled,
    )
    .await?;
    Ok(())
}

pub async fn start_periodic_scan(state: AppState, interval_seconds: u64) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_seconds));
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
            "SELECT id FROM movies WHERE external_imdb = ? LIMIT 1",
        )
        .bind(imdb)
        .fetch_optional(pool)
        .await?;
    }
    if existing.is_none() {
        if let Some(tvdb) = merged_ids.tvdb_movie.as_ref().or(merged_ids.tvdb.as_ref()) {
            existing = sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT movie_id FROM movie_external_ids WHERE provider = 'tvdb' AND external_id = ? LIMIT 1",
        )
        .bind(tvdb)
        .fetch_optional(pool)
            .await?;
        }
    }
    if existing.is_none() {
        if let Some(tmdb) = merged_ids.tmdb.as_ref() {
            existing = sqlx::query_scalar::<sqlx::Any, String>(
                "SELECT id FROM movies WHERE external_tmdb = ? LIMIT 1",
            )
            .bind(tmdb)
            .fetch_optional(pool)
            .await?;
        }
    }
    if existing.is_none() {
        existing = sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT id FROM movies WHERE title = ? AND (year IS ? OR year = ?) LIMIT 1",
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
            "UPDATE movies SET title = ?, year = ?, external_imdb = ?, external_tmdb = ?, metadata_json = COALESCE(?, metadata_json), runtime_seconds = COALESCE(?, runtime_seconds), updated_at = CURRENT_TIMESTAMP WHERE id = ?",
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
        let by_external = sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT series_id FROM series_external_ids WHERE provider = 'anilist' AND external_id = ? LIMIT 1",
        )
        .bind(anilist)
        .fetch_optional(pool)
        .await?;
        if by_external.is_some() {
            by_external
        } else {
            sqlx::query_scalar::<sqlx::Any, String>(
                "SELECT id FROM series WHERE external_anilist = ? LIMIT 1",
            )
            .bind(anilist)
            .fetch_optional(pool)
            .await?
        }
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
        // Fallback: match by title, allowing for loose type/year matching
        let rows = sqlx::query("SELECT id, year FROM series WHERE title = ?")
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
        best_match
    };

    if let Some(id_str) = existing {
        let id = Uuid::parse_str(&id_str)?;
        let identity_lock = load_managed_identity_lock(pool, &id_str).await?;
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
            "UPDATE series SET title = ?, year = ?, library_type = ?, external_imdb = ?, external_tvdb_series = ?, external_anilist = ?, metadata_json = COALESCE(?, metadata_json), updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        )
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
        sqlx::query::<sqlx::Any>("UPDATE seasons SET updated_at = CURRENT_TIMESTAMP WHERE id = ?")
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
    let existing =
        sqlx::query_scalar::<sqlx::Any, String>("SELECT id FROM media_items WHERE id = ? LIMIT 1")
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

#[derive(Debug, Clone, Copy)]
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

async fn link_movie_file(pool: &AnyPool, movie_id: Uuid, media_file_id: Uuid) -> Result<()> {
    let media_file_id_str = media_file_id.to_string();
    unlink_episode_links_for_media_file(pool, &media_file_id_str).await?;
    unlink_stale_movie_links_for_media_file(pool, &media_file_id_str, &movie_id.to_string())
        .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO movie_files (movie_id, media_file_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
    )
    .bind(movie_id.to_string())
    .bind(media_file_id_str)
    .execute(pool)
    .await?;
    Ok(())
}

async fn unlink_stale_movie_links_for_media_file(
    pool: &AnyPool,
    media_file_id: &str,
    keep_movie_id: &str,
) -> Result<()> {
    let stale_movie_ids: Vec<String> = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT movie_id FROM movie_files WHERE media_file_id = ? AND movie_id != ?",
    )
    .bind(media_file_id)
    .bind(keep_movie_id)
    .fetch_all(pool)
    .await?;
    if stale_movie_ids.is_empty() {
        return Ok(());
    }
    tracing::warn!(
        media_file_id = %media_file_id,
        stale_movie_count = stale_movie_ids.len(),
        "removing stale movie links for reclassified file"
    );
    sqlx::query::<sqlx::Any>("DELETE FROM movie_files WHERE media_file_id = ? AND movie_id != ?")
        .bind(media_file_id)
        .bind(keep_movie_id)
        .execute(pool)
        .await?;
    for movie_id in stale_movie_ids {
        cleanup_orphan_movie(pool, &movie_id).await?;
    }
    Ok(())
}

async fn link_episode_file(pool: &AnyPool, episode_id: Uuid, media_file_id: Uuid) -> Result<()> {
    let media_file_id_str = media_file_id.to_string();
    unlink_movie_links_for_media_file(pool, &media_file_id_str).await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO episode_files (episode_id, media_file_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
    )
    .bind(episode_id.to_string())
    .bind(media_file_id_str)
    .execute(pool)
    .await?;
    Ok(())
}

async fn mark_series_as_anime(pool: &AnyPool, series_id: Uuid) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE series SET library_type = 'anime', updated_at = CURRENT_TIMESTAMP WHERE id = ? AND library_type != 'anime'",
    )
    .bind(series_id.to_string())
    .execute(pool)
    .await?;
    let _ = sqlx::query::<sqlx::Any>(
        "UPDATE media_items SET type = 'anime', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
    )
    .bind(series_id.to_string())
    .execute(pool)
    .await;
    Ok(())
}

async fn unlink_movie_links_for_media_file(pool: &AnyPool, media_file_id: &str) -> Result<()> {
    let movie_ids: Vec<String> = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT movie_id FROM movie_files WHERE media_file_id = ?",
    )
    .bind(media_file_id)
    .fetch_all(pool)
    .await?;
    if movie_ids.is_empty() {
        return Ok(());
    }
    tracing::warn!(
        media_file_id = %media_file_id,
        movie_count = movie_ids.len(),
        "removing movie links for episode-classified file"
    );
    sqlx::query::<sqlx::Any>("DELETE FROM movie_files WHERE media_file_id = ?")
        .bind(media_file_id)
        .execute(pool)
        .await?;
    for movie_id in movie_ids {
        cleanup_orphan_movie(pool, &movie_id).await?;
    }
    Ok(())
}

async fn unlink_episode_links_for_media_file(pool: &AnyPool, media_file_id: &str) -> Result<()> {
    let has_episode: Option<i64> = sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT 1 FROM episode_files WHERE media_file_id = ? LIMIT 1",
    )
    .bind(media_file_id)
    .fetch_optional(pool)
    .await?;
    if has_episode.is_none() {
        return Ok(());
    }
    tracing::warn!(
        media_file_id = %media_file_id,
        "removing episode links for movie-classified file"
    );
    sqlx::query::<sqlx::Any>("DELETE FROM episode_files WHERE media_file_id = ?")
        .bind(media_file_id)
        .execute(pool)
        .await?;
    Ok(())
}

async fn cleanup_orphan_movie(pool: &AnyPool, movie_id: &str) -> Result<()> {
    let has_file: Option<i64> = sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT 1 FROM movie_files WHERE movie_id = ? LIMIT 1",
    )
    .bind(movie_id)
    .fetch_optional(pool)
    .await?;
    if has_file.is_some() {
        return Ok(());
    }
    tracing::info!(movie_id = %movie_id, "removing orphan movie");
    sqlx::query::<sqlx::Any>("DELETE FROM movies WHERE id = ?")
        .bind(movie_id)
        .execute(pool)
        .await?;
    let _ = sqlx::query::<sqlx::Any>("DELETE FROM media_items WHERE id = ?")
        .bind(movie_id)
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

async fn update_episode_title_if_missing(
    pool: &AnyPool,
    episode_id: Uuid,
    title: &str,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE episodes SET title = COALESCE(title, ?), updated_at = CURRENT_TIMESTAMP WHERE id = ?",
    )
    .bind(title)
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
    tracing::trace!(
        series_id = %series_id,
        source,
        ids = ?ids,
        "apply external ids to series"
    );
    let tvdb = ids.tvdb_series.as_ref().or(ids.tvdb.as_ref()).cloned();
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

pub(crate) async fn apply_external_ids_to_season(
    pool: &AnyPool,
    season_id: Uuid,
    ids: &ExternalIds,
    source: &str,
    confidence: Option<f32>,
) -> Result<()> {
    tracing::trace!(
        season_id = %season_id,
        source,
        ids = ?ids,
        "apply external ids to season"
    );
    if let Some(new_id) = ids.anilist.as_ref() {
        let existing = sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT COALESCE(external_anilist, '') FROM seasons WHERE id = ? LIMIT 1",
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
            } else if let Some(new_confidence) = confidence {
                let existing_confidence = sqlx::query_scalar::<sqlx::Any, f32>(
                    "SELECT MAX(confidence) FROM season_external_ids WHERE season_id = ? AND provider = 'anilist' AND external_id = ?",
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
                "UPDATE seasons SET external_anilist = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            )
            .bind(new_id)
            .bind(season_id.to_string())
            .execute(pool)
            .await?;
        }
    }

    persist_season_external_ids(pool, season_id, ids, source, confidence).await?;
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

    let stored_count = entries.len();
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

    let stored_count = entries.len();
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

    tracing::trace!(
        series_id = %series_id,
        source,
        stored = stored_count,
        "persisted series external ids"
    );
    Ok(())
}

async fn persist_season_external_ids(
    pool: &AnyPool,
    season_id: Uuid,
    ids: &ExternalIds,
    source: &str,
    confidence: Option<f32>,
) -> Result<()> {
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
    for (provider, external_id) in entries {
        let stored_confidence = if provider == "anilist" {
            confidence.unwrap_or(1.0)
        } else {
            1.0
        };
        sqlx::query::<sqlx::Any>(
            "INSERT INTO season_external_ids (id, season_id, provider, external_id, confidence, source) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(season_id.to_string())
        .bind(provider)
        .bind(external_id)
        .bind(stored_confidence)
        .bind(source)
        .execute(pool)
        .await?;
    }

    tracing::trace!(
        season_id = %season_id,
        source,
        stored = stored_count,
        "persisted season external ids"
    );
    Ok(())
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
    if season_scaffolded_recent(pool, season_id, Some("anizip"), ttl_seconds, force_metadata)
        .await?
    {
        return Ok(());
    }
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
        upsert_anime_episode_meta(pool, season_id, ep_number, episode).await?;
        if let (Some(artwork_service), Some(url)) = (artwork, episode.image.as_deref()) {
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
        "SELECT COALESCE(CAST(metadata_json AS TEXT), '') FROM series WHERE id = ? LIMIT 1",
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
        "UPDATE series SET metadata_json = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
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
            "SELECT COALESCE(CAST(metadata_json AS TEXT), '') FROM seasons WHERE id = ? LIMIT 1",
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
            "UPDATE seasons SET title = COALESCE(?, title), metadata_json = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
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

async fn season_scaffolded(pool: &AnyPool, season_id: Uuid) -> Result<bool> {
    let meta = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT COALESCE(CAST(metadata_json AS TEXT), '') FROM seasons WHERE id = ? LIMIT 1",
    )
    .bind(season_id.to_string())
    .fetch_optional(pool)
    .await?;
    let meta = meta.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    });
    let Some(meta) = meta else {
        return Ok(false);
    };
    let parsed: serde_json::Value = serde_json::from_str(&meta).unwrap_or_default();
    Ok(parsed
        .get("scaffolded")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false))
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
         FROM seasons WHERE id = ? LIMIT 1",
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
        sqlx::query_scalar::<sqlx::Any, String>("SELECT id FROM seasons WHERE series_id = ?")
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
        "SELECT COALESCE(CAST(metadata_json AS TEXT), '') FROM seasons WHERE id = ? LIMIT 1",
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
         WHERE media_item_id = ?
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
mod tests {
    use super::*;
    use crate::{
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
    use axum::{
        Json, Router,
        body::Body,
        extract::Path as AxumPath,
        http::StatusCode,
        response::IntoResponse,
        routing::{get, post},
    };
    use serde_json::Value;
    use std::collections::HashMap;
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

        let (queue_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM review_queue")
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
        let normalized = derive_override_key("movie", media_path).expect("override key");

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
        let external_ids_json = serde_json::json!({
            "imdb": "tt0096256"
        });

        sqlx::query(
            "INSERT INTO managed_ingest_intents (
                intent_id, media_type, title, normalized_title, year, external_ids_json,
                manager_provider_id, manager_item_id, manager_label, source, active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind("movie")
        .bind(title)
        .bind(normalized_title)
        .bind(1988)
        .bind(serde_json::to_string(&external_ids_json)?)
        .bind(Uuid::new_v4().to_string())
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
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
        run_full_scan(&database.pool, noisy_scan, false).await?;

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
            "SELECT COUNT(*) FROM artwork_refs WHERE owner_type = 'movie' AND owner_id = ?",
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
            "SELECT external_id FROM movie_external_ids WHERE movie_id = ? AND provider = 'tvdb' LIMIT 1",
        )
        .bind(movie_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(tvdb_movie_id, "12345");

        let tvdb_artwork_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM artwork_refs WHERE owner_type = 'movie' AND owner_id = ? AND provider = 'tvdb'",
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
        run_full_scan(&database.pool, noisy_scan, false).await?;

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
        let external_ids_json = serde_json::json!({
            "anilist": "151807"
        });

        sqlx::query(
            "INSERT INTO managed_ingest_intents (
                intent_id, media_type, title, normalized_title, year, external_ids_json,
                manager_provider_id, manager_item_id, manager_label, source, active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind("anime")
        .bind(title)
        .bind(normalized_title)
        .bind(2024)
        .bind(serde_json::to_string(&external_ids_json)?)
        .bind(Uuid::new_v4().to_string())
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
        run_full_scan(&database.pool, candidates, false).await?;

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
}
