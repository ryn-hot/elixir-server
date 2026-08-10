use super::*;

use crate::{
    config::{ClassifierConfig, DatabaseConfig, MetadataConfig},
    db::Database,
    extensions::{ExternalIds as ExtIds, FileDescriptor as FD, MediaIdentity},
};
use anyhow::Context;
use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use elixir_classifier::{
    hint::{ClassificationHint, FileInput as ClassifierInput, HintParser, LibraryType},
    identify::{CandidateMatch, IdentifierProvider, KindHint, MatchFeatures},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tempfile::tempdir;
use tokio::{net::TcpListener, sync::oneshot};

const SERIES_TITLE: &str = "Tokyo Ghoul";

#[derive(Clone)]
struct PathHintParser {
    hints: Arc<HashMap<String, ClassificationHint>>,
}

impl HintParser for PathHintParser {
    fn name(&self) -> &'static str {
        "alm2_path_fixture"
    }

    fn parse(&self, input: &ClassifierInput) -> Vec<ClassificationHint> {
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
struct CandidateSpec {
    title: String,
    anilist_id: String,
    season: Option<i32>,
    episode: Option<i32>,
    absolute_episode: Option<i32>,
}

#[derive(Clone)]
struct PathIdentifier {
    candidates: Arc<HashMap<String, CandidateSpec>>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl IdentifierProvider for PathIdentifier {
    fn name(&self) -> &'static str {
        "alm2_candidate_fixture"
    }

    fn supports(&self, _library_type: LibraryType) -> bool {
        true
    }

    async fn identify(&self, hint: &ClassificationHint) -> Result<Vec<CandidateMatch>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let Some(path) = hint.source_path.as_ref() else {
            return Ok(Vec::new());
        };
        let Some(spec) = self.candidates.get(path) else {
            return Ok(Vec::new());
        };
        Ok(vec![CandidateMatch {
            provider: "alm2_candidate_fixture",
            kind: KindHint::Anime,
            ids: ClassifierExternalIds {
                anilist: Some(spec.anilist_id.clone()),
                ..Default::default()
            },
            input_echo: false,
            title: spec.title.clone(),
            alt_titles: Vec::new(),
            year: hint.year,
            season: spec.season,
            episode: spec.episode,
            absolute_episode: spec.absolute_episode,
            duration_seconds: None,
            provider_confidence: 1.0,
            score: 0.0,
            features: MatchFeatures::default(),
        }])
    }
}

#[derive(Clone)]
struct AniZipFixtureState {
    online: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<String>>>,
}

async fn anizip_fixture_handler(
    State(state): State<AniZipFixtureState>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let anilist_id = query.get("anilist_id").cloned().unwrap_or_default();
    state
        .requests
        .lock()
        .expect("ani.zip request lock poisoned")
        .push(anilist_id.clone());

    if !state.online.load(Ordering::SeqCst) {
        return (StatusCode::SERVICE_UNAVAILABLE, "fixture outage").into_response();
    }

    if anilist_id == "metadata-only-refresh" {
        return Json(json!({
            "mappings": {
                "anilist_id": anilist_id,
                "thetvdb_id": "305014"
            },
            "episodes": {},
            "images": [],
            "titles": { "en": "Metadata-Only Refresh" }
        }))
        .into_response();
    }

    let episodes = match anilist_id.as_str() {
        "tokyo-ghoul-root-a" => json!({
            "13": mapped_episode(2, 1, 13, "New Surge"),
            "14": mapped_episode(2, 2, 14, "Dancing Flowers")
        }),
        "long-s1" => json!({
            "1": mapped_episode(1, 1, 1, "The Beginning"),
            "12": mapped_episode(1, 12, 12, "Season One Finale")
        }),
        "long-s2" => json!({
            "13": mapped_episode(2, 1, 13, "Second Season Premiere"),
            "24": mapped_episode(2, 12, 24, "Second Season Finale")
        }),
        "long-s9" => json!({
            "101": mapped_episode(9, 5, 101, "One Hundred and One")
        }),
        "shared-retry-id" => json!({
            "13": mapped_episode(2, 1, 13, "Retry Season Two"),
            "25": mapped_episode(3, 1, 25, "Retry Season Three")
        }),
        _ => return StatusCode::NOT_FOUND.into_response(),
    };

    Json(json!({
        "mappings": {
            "anilist_id": anilist_id,
            "thetvdb_id": "305014"
        },
        "episodes": episodes,
        "images": [],
        "titles": {
            "en": SERIES_TITLE
        }
    }))
    .into_response()
}

fn mapped_episode(season: i32, episode: i32, absolute: i32, title: &str) -> Value {
    json!({
        "episode": absolute.to_string(),
        "seasonNumber": season,
        "episodeNumber": episode,
        "absoluteEpisodeNumber": absolute,
        "title": { "en": title }
    })
}

async fn start_anizip_fixture(
    online: bool,
) -> Result<(
    String,
    Arc<AtomicBool>,
    Arc<Mutex<Vec<String>>>,
    oneshot::Sender<()>,
)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let online = Arc::new(AtomicBool::new(online));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let state = AniZipFixtureState {
        online: online.clone(),
        requests: requests.clone(),
    };
    let app = Router::new()
        .route("/mappings", get(anizip_fixture_handler))
        .with_state(state);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });
    Ok((format!("http://{address}"), online, requests, shutdown_tx))
}

fn absolute_hint(title: &str, absolute_episode: i32) -> ClassificationHint {
    ClassificationHint {
        library_type: LibraryType::Anime,
        title: title.to_string(),
        alt_titles: Vec::new(),
        year: Some(2014),
        season: None,
        episode: None,
        absolute_episode: Some(absolute_episode),
        duration_seconds: None,
        embedded_ids: ClassifierExternalIds::default(),
        parser: "alm2_path_fixture",
        parser_confidence: 1.0,
        source_path: None,
    }
}

fn explicit_hint(title: &str, season: i32, episode: i32) -> ClassificationHint {
    ClassificationHint {
        season: Some(season),
        episode: Some(episode),
        absolute_episode: None,
        ..absolute_hint(title, 1)
    }
}

fn classifier_pipeline(
    fixtures: Vec<(String, ClassificationHint, CandidateSpec)>,
) -> (ClassifierPipeline, Arc<AtomicUsize>) {
    let mut hints = HashMap::new();
    let mut candidates = HashMap::new();
    for (path, hint, candidate) in fixtures {
        hints.insert(path.clone(), hint);
        candidates.insert(path, candidate);
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let pipeline = ClassifierPipeline::new()
        .register_hint_parser(Arc::new(PathHintParser {
            hints: Arc::new(hints),
        }))
        .register_identifier_provider(Arc::new(PathIdentifier {
            candidates: Arc::new(candidates),
            calls: calls.clone(),
        }));
    (pipeline, calls)
}

fn absolute_candidate(
    title: &str,
    anilist_id: &str,
    season: i32,
    absolute_episode: i32,
) -> CandidateSpec {
    CandidateSpec {
        title: title.to_string(),
        anilist_id: anilist_id.to_string(),
        season: Some(season),
        episode: None,
        absolute_episode: Some(absolute_episode),
    }
}

fn scan_candidate(path: &str, season: Option<i32>, episode: Option<i32>) -> MediaFileCandidate {
    scan_candidate_with_title(path, SERIES_TITLE, season, episode)
}

fn scan_candidate_with_title(
    path: &str,
    title: &str,
    season: Option<i32>,
    episode: Option<i32>,
) -> MediaFileCandidate {
    MediaFileCandidate {
        identity: MediaIdentity {
            r#type: MediaType::Anime,
            external_ids: ExtIds::default(),
            title: title.to_string(),
            year: Some(2014),
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

fn disabled_metadata_with_ttl() -> Result<MetadataService> {
    MetadataService::new(MetadataConfig {
        enable_tvdb: false,
        enable_cinemeta: false,
        enable_anilist: false,
        enable_aniapi: false,
        enable_consumet: false,
        ttl_seconds: 86_400,
        ..MetadataConfig::default()
    })
}

fn linker_config(anizip_base_url: String) -> ClassifierConfig {
    ClassifierConfig {
        tvdb_base_url: String::new(),
        tvdb_api_key: None,
        anizip_base_url,
        request_timeout_seconds: 1,
    }
}

async fn database() -> Result<Database> {
    let database = Database::connect(&DatabaseConfig {
        url: "sqlite::memory:".to_string(),
        max_connections: 1,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    Ok(database)
}

async fn linked_numbers(pool: &AnyPool, path: &str) -> Result<Option<(i64, i64, Option<i64>)>> {
    let rows = sqlx::query_as(
        "SELECT e.season_number, e.episode_number, e.absolute_episode_number
         FROM episodes e
         JOIN episode_files ef ON ef.episode_id = e.id
         JOIN media_files mf ON mf.id = ef.media_file_id
         WHERE mf.path = $1",
    )
    .bind(path)
    .fetch_all(pool)
    .await?;
    if rows.len() > 1 {
        anyhow::bail!("media file {path} has {} episode links", rows.len());
    }
    Ok(rows.into_iter().next())
}

async fn disposition(pool: &AnyPool, path: &str) -> Result<Option<String>> {
    Ok(sqlx::query_scalar(
        "SELECT crs.disposition
         FROM classifier_resolution_state crs
         JOIN media_files mf ON mf.id = crs.media_file_id
         WHERE mf.path = $1",
    )
    .bind(path)
    .fetch_optional(pool)
    .await?)
}

fn request_count(requests: &Arc<Mutex<Vec<String>>>, anilist_id: &str) -> usize {
    requests
        .lock()
        .expect("ani.zip request lock poisoned")
        .iter()
        .filter(|value| value.as_str() == anilist_id)
        .count()
}

fn in_memory_mapping(season: i32, episode: i32, absolute: i32) -> AniZipMapping {
    AniZipMapping {
        ids: ExtIds::default(),
        episodes: vec![AniZipEpisodeRecord {
            season_number: Some(season),
            episode_number: Some(episode),
            absolute_episode_number: Some(absolute),
            episode_label: Some(absolute.to_string()),
            mainline_episode_number: Some(absolute),
            title: None,
            overview: None,
            runtime_minutes: None,
            image: None,
            tvdb_id: None,
            anidb_eid: None,
            raw: mapped_episode(season, episode, absolute, "Fixture Episode"),
        }],
        images: Vec::new(),
        titles: HashMap::new(),
    }
}

#[test]
fn alm2_resolver_rejects_ambiguous_and_partial_season_mappings() {
    let file = AggregatedFile {
        descriptor: FD {
            path: "/media/ambiguous-absolute.mkv".to_string(),
            size_bytes: None,
            hash: None,
            container: None,
            video_codec: None,
            audio_codec: None,
        },
        source_config_id: None,
        extension_metadata: HashMap::new(),
        season: None,
        episode: None,
        absolute_episode: None,
    };
    let applied = |season, absolute_episode| ClassificationOutcome {
        disposition: ClassificationDisposition::Applied,
        confidence: Some(1.0),
        hint_json: None,
        candidates_json: None,
        season_scope: season,
        retry_supersedes_applied: false,
        bridge_protected: false,
        parsed_hint: None,
        accepted_numbers: Some(ResolvedEpisodeNumbers {
            season,
            episode: None,
            absolute_episode: Some(absolute_episode),
        }),
        preserve_authoritative_episode_links: false,
        applied_identity_rows: Default::default(),
    };

    let mut ambiguous = CanonicalEpisodeNumberMap::new();
    insert_anizip_episode_numbers(&mut ambiguous, &in_memory_mapping(1, 1, 1));
    insert_anizip_episode_numbers(&mut ambiguous, &in_memory_mapping(2, 1, 1));
    let resolved =
        resolve_episode_numbers(&file, Some(&applied(None, 1)), MediaType::Anime, &ambiguous);
    assert_eq!(
        resolved.season, None,
        "ambiguous absolute mapping must not choose a season"
    );
    assert_eq!(
        resolved.episode, None,
        "ambiguous absolute mapping must not choose an episode"
    );
    assert_eq!(resolved.absolute_episode, Some(1));

    let mut wrong_season_only = CanonicalEpisodeNumberMap::new();
    insert_anizip_episode_numbers(&mut wrong_season_only, &in_memory_mapping(1, 13, 13));
    let resolved = resolve_episode_numbers(
        &file,
        Some(&applied(Some(2), 13)),
        MediaType::Anime,
        &wrong_season_only,
    );
    assert_eq!(resolved.season, Some(2));
    assert_eq!(
        resolved.episode, None,
        "an S01 mapping must not be spliced into a known S02 result"
    );
    assert_eq!(resolved.absolute_episode, Some(13));
}

#[test]
fn alm2_full_mapping_context_maps_season_titles_and_artwork() {
    let mut mapping = in_memory_mapping(2, 1, 13);
    mapping.titles = HashMap::from([
        ("ja".to_string(), "東京喰種トーキョーグール√A".to_string()),
        ("en".to_string(), "Tokyo Ghoul Root A".to_string()),
    ]);
    mapping.images = serde_json::from_value(json!([
        { "coverType": "Poster", "url": "https://images.example/poster.jpg" },
        { "coverType": "Fanart", "url": "https://images.example/fanart.jpg" },
        { "coverType": "Banner", "url": "https://images.example/banner.jpg" },
        { "coverType": "Thumbnail", "url": "https://images.example/thumb.jpg" },
        { "coverType": "Logo", "url": "https://images.example/logo.png" }
    ]))
    .expect("ani.zip image fixture");

    assert_eq!(preferred_anizip_title(&mapping), Some("Tokyo Ghoul Root A"));
    assert!(anizip_mapping_contains_season(&mapping, 2));
    assert!(!anizip_mapping_contains_season(&mapping, 1));
    let candidates = anizip_artwork_candidates(&mapping);
    assert_eq!(candidates.len(), 4);
    assert_eq!(candidates[0].kind, ArtworkKind::Poster);
    assert_eq!(candidates[1].kind, ArtworkKind::Backdrop);
    assert_eq!(candidates[2].kind, ArtworkKind::Banner);
    assert_eq!(candidates[3].kind, ArtworkKind::Thumbnail);
    assert!(
        candidates
            .iter()
            .all(|candidate| candidate.provider.as_deref() == Some("anizip"))
    );

    let mut partial = mapping.clone();
    partial.episodes[0].episode_number = None;
    assert_eq!(
        infer_anizip_mapping_season(&partial),
        None,
        "a record without a usable seasonal episode cannot establish mapping ownership"
    );
}

#[test]
fn alm2_current_mapping_supersedes_only_conflicting_persisted_numbers() {
    let mut persisted = CanonicalEpisodeNumberMap::new();
    persisted.insert(
        2,
        vec![
            CanonicalEpisodeNumber {
                season: 2,
                episode: 1,
                absolute_episode: 13,
            },
            CanonicalEpisodeNumber {
                season: 2,
                episode: 3,
                absolute_episode: 14,
            },
        ],
    );
    persisted.insert(
        4,
        vec![CanonicalEpisodeNumber {
            season: 4,
            episode: 1,
            absolute_episode: 99,
        }],
    );

    let mut current = CanonicalEpisodeNumberMap::new();
    current.insert(
        2,
        vec![CanonicalEpisodeNumber {
            season: 2,
            episode: 2,
            absolute_episode: 13,
        }],
    );
    current.insert(
        4,
        vec![CanonicalEpisodeNumber {
            season: 4,
            episode: 1,
            absolute_episode: 50,
        }],
    );
    let merged = merge_authoritative_anizip_numbers(persisted.clone(), current.clone());
    assert_eq!(
        lookup_canonical_absolute_episode(&merged, None, None, 13),
        Some((2, 2))
    );
    assert_eq!(
        lookup_canonical_absolute_episode(&merged, None, None, 14),
        Some((2, 3)),
        "a partial current response must retain unrelated persisted evidence"
    );
    assert_eq!(
        lookup_canonical_absolute_episode(&merged, None, None, 99),
        None,
        "a corrected canonical slot must replace its former absolute number"
    );
    assert_eq!(
        lookup_canonical_absolute_episode(&merged, None, None, 50),
        Some((4, 1))
    );

    current.entry(1).or_default().push(CanonicalEpisodeNumber {
        season: 1,
        episode: 13,
        absolute_episode: 13,
    });
    let ambiguous = merge_authoritative_anizip_numbers(persisted, current);
    assert_eq!(
        lookup_canonical_absolute_episode(&ambiguous, None, None, 13),
        None,
        "conflicting current mappings must remain ambiguous"
    );
}

#[test]
fn alm2_series_external_ids_come_only_from_the_selected_root_mapping() {
    let base = ExtIds {
        anilist: Some("root-200".to_string()),
        ..Default::default()
    };
    let root = AniZipMapping {
        ids: ExtIds {
            imdb: Some("tt-root".to_string()),
            tmdb: Some("tmdb-root".to_string()),
            tvdb_series: Some("tvdb-root".to_string()),
            anilist: Some("root-200".to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    let season = AniZipMapping {
        ids: ExtIds {
            imdb: Some("tt-season".to_string()),
            tmdb: Some("tmdb-season".to_string()),
            tvdb_series: Some("tvdb-season".to_string()),
            anilist: Some("season-100".to_string()),
            ..Default::default()
        },
        ..Default::default()
    };

    for mappings in [
        HashMap::from([
            ("season-100".to_string(), Arc::new(season.clone())),
            ("root-200".to_string(), Arc::new(root.clone())),
        ]),
        HashMap::from([
            ("root-200".to_string(), Arc::new(root.clone())),
            ("season-100".to_string(), Arc::new(season.clone())),
        ]),
    ] {
        let merged = merge_root_anizip_external_ids(&base, &mappings);
        assert_eq!(merged.imdb.as_deref(), Some("tt-root"));
        assert_eq!(merged.tmdb.as_deref(), Some("tmdb-root"));
        assert_eq!(merged.tvdb_series.as_deref(), Some("tvdb-root"));
        assert_eq!(merged.anilist.as_deref(), Some("root-200"));
    }

    let season_only = HashMap::from([("season-100".to_string(), Arc::new(season))]);
    assert_eq!(merge_root_anizip_external_ids(&base, &season_only), base);

    let authoritative = ExtIds {
        imdb: Some("tt-existing".to_string()),
        anilist: Some("root-200".to_string()),
        ..Default::default()
    };
    let root_only = HashMap::from([("root-200".to_string(), Arc::new(root))]);
    let merged = merge_root_anizip_external_ids(&authoritative, &root_only);
    assert_eq!(merged.imdb.as_deref(), Some("tt-existing"));
    assert_eq!(merged.tvdb_series.as_deref(), Some("tvdb-root"));
}

#[test]
fn alm2_equal_confidence_anilist_seed_conflicts_are_suppressed() {
    let mut seeds = HashMap::new();
    insert_season_anilist_seed(
        &mut seeds,
        2,
        SeasonAnilistSeed {
            anilist_id: "200".to_string(),
            confidence: 0.9,
            causal_paths: Default::default(),
        },
    );
    insert_season_anilist_seed(
        &mut seeds,
        2,
        SeasonAnilistSeed {
            anilist_id: "100".to_string(),
            confidence: 0.9,
            causal_paths: Default::default(),
        },
    );
    assert!(!season_anilist_seed_is_usable(&seeds[&2]));

    let base = ExtIds::default();
    let mut merged = ExtIds {
        anilist: Some("200".to_string()),
        ..Default::default()
    };
    suppress_conflicting_classifier_anilist_id(&base, &mut merged, &seeds, false);
    assert_eq!(merged.anilist, None);

    let explicit_base = ExtIds {
        anilist: Some("managed-root".to_string()),
        ..Default::default()
    };
    let mut explicit_merged = explicit_base.clone();
    suppress_conflicting_classifier_anilist_id(&explicit_base, &mut explicit_merged, &seeds, false);
    assert_eq!(explicit_merged.anilist.as_deref(), Some("managed-root"));

    insert_season_anilist_seed(
        &mut seeds,
        2,
        SeasonAnilistSeed {
            anilist_id: "300".to_string(),
            confidence: 0.95,
            causal_paths: Default::default(),
        },
    );
    assert!(season_anilist_seed_is_usable(&seeds[&2]));
    assert_eq!(seeds[&2].anilist_id, "300");
}

#[tokio::test]
async fn alm2_persisted_number_map_requires_anizip_provenance() -> Result<()> {
    let database = database().await?;
    let identity = MediaIdentity {
        r#type: MediaType::Anime,
        external_ids: ExtIds::default(),
        title: "Historical Poison Fixture".to_string(),
        year: Some(2014),
        season: None,
        episode: None,
    };
    let series_id = upsert_series(&database.pool, &identity, &ExtIds::default(), None).await?;
    let season_id = upsert_season(&database.pool, series_id, 1).await?;
    upsert_episode(&database.pool, series_id, season_id, 1, 1, Some(13)).await?;

    let unproven = load_persisted_episode_number_map(&database.pool, series_id).await?;
    assert_eq!(
        lookup_canonical_absolute_episode(&unproven, None, None, 13),
        None,
        "a historical episode row without ani.zip provenance must never become canonical"
    );

    let mapping = in_memory_mapping(1, 1, 13);
    upsert_anime_episode_meta(&database.pool, season_id, 1, &mapping.episodes[0]).await?;
    sqlx::query(
        "UPDATE episodes SET absolute_episode_number = 99 \
         WHERE series_id = $1 AND season_number = 1 AND episode_number = 1",
    )
    .bind(series_id.to_string())
    .execute(&database.pool)
    .await?;
    let proven = load_persisted_episode_number_map(&database.pool, series_id).await?;
    assert_eq!(
        lookup_canonical_absolute_episode(&proven, None, None, 13),
        Some((1, 1))
    );
    assert_eq!(
        lookup_canonical_absolute_episode(&proven, None, None, 99),
        None,
        "mutable normalized episode rows must not poison ani.zip-owned numbering"
    );
    Ok(())
}

#[tokio::test]
async fn alm2_cached_correction_supersedes_stale_persisted_number_evidence() -> Result<()> {
    let database = database().await?;
    let ids = ExtIds {
        anilist: Some("correction-id".to_string()),
        ..Default::default()
    };
    let identity = MediaIdentity {
        r#type: MediaType::Anime,
        external_ids: ids.clone(),
        title: "Corrected Mapping Fixture".to_string(),
        year: Some(2014),
        season: None,
        episode: None,
    };
    let series_id = upsert_series(&database.pool, &identity, &ids, None).await?;
    let season_id = upsert_season(&database.pool, series_id, 2).await?;
    let stale = in_memory_mapping(2, 1, 13);
    upsert_anime_episode_meta(&database.pool, season_id, 1, &stale.episodes[0]).await?;

    let mut corrected = in_memory_mapping(2, 2, 13);
    corrected.ids.anilist = Some("correction-id".to_string());
    persist_cached_anizip_mapping(&database.pool, "correction-id", &corrected).await?;

    let media_dir = tempdir()?;
    let path = media_dir
        .path()
        .join("Corrected Mapping Fixture - 13.mkv")
        .to_string_lossy()
        .to_string();
    std::fs::write(&path, b"fixture")?;
    let (pipeline, _calls) = classifier_pipeline(vec![(
        path.clone(),
        absolute_hint("Corrected Mapping Fixture", 13),
        CandidateSpec {
            title: "Corrected Mapping Fixture".to_string(),
            anilist_id: "correction-id".to_string(),
            season: None,
            episode: None,
            absolute_episode: Some(13),
        },
    )]);
    let metadata = disabled_metadata_with_ttl()?;
    let config = linker_config(String::new());

    run_full_scan_with_classifier(
        &database.pool,
        Some(&metadata),
        None,
        Some(&config),
        None,
        &pipeline,
        vec![scan_candidate_with_title(
            &path,
            "Corrected Mapping Fixture",
            None,
            None,
        )],
        false,
        false,
        false,
    )
    .await?;

    assert_eq!(
        linked_numbers(&database.pool, &path).await?,
        Some((2, 2, Some(13)))
    );
    Ok(())
}

#[tokio::test]
async fn alm2_concurrent_season_and_episode_upserts_converge_on_one_row() -> Result<()> {
    let temp = tempdir()?;
    let db_path = temp.path().join("alm2-concurrent-upsert.db");
    let database = Database::connect(&DatabaseConfig {
        url: format!("sqlite://{}", db_path.display()),
        max_connections: 4,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    let identity = MediaIdentity {
        r#type: MediaType::Anime,
        external_ids: ExtIds::default(),
        title: "Concurrent Anime Fixture".to_string(),
        year: Some(2014),
        season: None,
        episode: None,
    };
    let series_id = upsert_series(&database.pool, &identity, &ExtIds::default(), None).await?;

    let (season_a, season_b, season_c, season_d) = tokio::join!(
        upsert_season(&database.pool, series_id, 2),
        upsert_season(&database.pool, series_id, 2),
        upsert_season(&database.pool, series_id, 2),
        upsert_season(&database.pool, series_id, 2),
    );
    let season_ids = [season_a?, season_b?, season_c?, season_d?];
    assert!(season_ids.iter().all(|id| *id == season_ids[0]));

    let season_id = season_ids[0];
    let (episode_a, episode_b, episode_c, episode_d) = tokio::join!(
        upsert_episode(&database.pool, series_id, season_id, 2, 1, None),
        upsert_episode(&database.pool, series_id, season_id, 2, 1, Some(13)),
        upsert_episode(&database.pool, series_id, season_id, 2, 1, None),
        upsert_episode(&database.pool, series_id, season_id, 2, 1, Some(13)),
    );
    let episode_ids = [episode_a?, episode_b?, episode_c?, episode_d?];
    assert!(episode_ids.iter().all(|id| *id == episode_ids[0]));

    let mapping = in_memory_mapping(2, 1, 13);
    let record = &mapping.episodes[0];
    let (meta_a, meta_b, meta_c, meta_d) = tokio::join!(
        upsert_anime_episode_meta(&database.pool, season_id, 1, record),
        upsert_anime_episode_meta(&database.pool, season_id, 1, record),
        upsert_anime_episode_meta(&database.pool, season_id, 1, record),
        upsert_anime_episode_meta(&database.pool, season_id, 1, record),
    );
    meta_a?;
    meta_b?;
    meta_c?;
    meta_d?;

    let season_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM seasons WHERE series_id = $1 AND season_number = 2",
    )
    .bind(series_id.to_string())
    .fetch_one(&database.pool)
    .await?;
    let episode_state: (i64, Option<i64>) = sqlx::query_as(
        "SELECT COUNT(*), MAX(absolute_episode_number) FROM episodes \
         WHERE series_id = $1 AND season_number = 2 AND episode_number = 1",
    )
    .bind(series_id.to_string())
    .fetch_one(&database.pool)
    .await?;
    let anime_meta_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM anime_episode_meta \
         WHERE season_id = $1 AND episode_number = 1",
    )
    .bind(season_id.to_string())
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(season_count, 1);
    assert_eq!(episode_state, (1, Some(13)));
    assert_eq!(anime_meta_count, 1);
    Ok(())
}

#[tokio::test]
async fn alm2_empty_cache_entry_is_not_treated_as_fresh_mapping() -> Result<()> {
    let database = database().await?;
    let now = Utc::now().timestamp();
    sqlx::query(
        "INSERT INTO anizip_mapping_cache \
         (anilist_id, schema_version, mapping_json, fetched_at_epoch_seconds, \
          updated_at_epoch_seconds) VALUES ('empty-cache-id', 1, '{}', $1, $1)",
    )
    .bind(now)
    .execute(&database.pool)
    .await?;
    assert!(
        load_cached_anizip_mapping(&database.pool, "empty-cache-id", 86_400, false)
            .await?
            .is_none()
    );

    let metadata_only = AniZipMapping {
        ids: ExtIds {
            anilist: Some("metadata-only-id".to_string()),
            ..Default::default()
        },
        titles: HashMap::from([("en".to_string(), "Metadata Only".to_string())]),
        ..Default::default()
    };
    persist_cached_anizip_mapping(&database.pool, "metadata-only-id", &metadata_only).await?;
    let metadata_only_cache =
        load_cached_anizip_mapping(&database.pool, "metadata-only-id", 86_400, false)
            .await?
            .context("metadata-only cache remains available as stale fallback context")?;
    assert!(
        !metadata_only_cache.is_fresh,
        "metadata-only cache data must not suppress a retry for canonical episode numbers"
    );

    let absolute_only = AniZipMapping {
        episodes: vec![AniZipEpisodeRecord {
            absolute_episode_number: Some(13),
            raw: json!({ "episode": "13", "absoluteEpisodeNumber": 13 }),
            ..Default::default()
        }],
        ..Default::default()
    };
    persist_cached_anizip_mapping(&database.pool, "absolute-only-id", &absolute_only).await?;
    let absolute_only_cache =
        load_cached_anizip_mapping(&database.pool, "absolute-only-id", 86_400, false)
            .await?
            .context("absolute-only cache remains available as stale fallback evidence")?;
    assert!(!absolute_only_cache.is_fresh);
    Ok(())
}

#[tokio::test]
async fn alm2_stale_persisted_mapping_survives_refresh_outage() -> Result<()> {
    let database = database().await?;
    let mapping = in_memory_mapping(2, 1, 13);
    persist_cached_anizip_mapping(&database.pool, "stale-cache-id", &mapping).await?;
    sqlx::query(
        "UPDATE anizip_mapping_cache SET fetched_at_epoch_seconds = 1 \
         WHERE anilist_id = 'stale-cache-id'",
    )
    .execute(&database.pool)
    .await?;

    let (base_url, _online, requests, shutdown) = start_anizip_fixture(false).await?;
    let linker = LinkerService::new(linker_config(base_url))?;
    let mut scan_cache = HashMap::new();
    let resolved = anizip_mapping_for_scan(
        &database.pool,
        Some(&linker),
        "stale-cache-id",
        60,
        false,
        &mut scan_cache,
    )
    .await?
    .context("stale mapping should remain available after refresh failure")?;
    assert_eq!(resolved.episodes.len(), 1);
    assert_eq!(resolved.episodes[0].absolute_episode_number, Some(13));
    assert_eq!(request_count(&requests, "stale-cache-id"), 1);

    let second = anizip_mapping_for_scan(
        &database.pool,
        Some(&linker),
        "stale-cache-id",
        60,
        false,
        &mut scan_cache,
    )
    .await?;
    let _ = shutdown.send(());
    assert!(second.is_some());
    assert_eq!(request_count(&requests, "stale-cache-id"), 1);
    Ok(())
}

#[tokio::test]
async fn alm2_incomplete_refresh_retains_stale_canonical_mapping() -> Result<()> {
    let database = database().await?;
    let mapping = in_memory_mapping(2, 1, 13);
    persist_cached_anizip_mapping(&database.pool, "metadata-only-refresh", &mapping).await?;
    sqlx::query(
        "UPDATE anizip_mapping_cache SET fetched_at_epoch_seconds = 1 \
         WHERE anilist_id = 'metadata-only-refresh'",
    )
    .execute(&database.pool)
    .await?;

    let (base_url, _online, requests, shutdown) = start_anizip_fixture(true).await?;
    let linker = LinkerService::new(linker_config(base_url))?;
    let mut scan_cache = HashMap::new();
    let resolved = anizip_mapping_for_scan(
        &database.pool,
        Some(&linker),
        "metadata-only-refresh",
        60,
        false,
        &mut scan_cache,
    )
    .await?
    .context("stale canonical mapping should survive an incomplete refresh")?;
    let _ = shutdown.send(());

    assert!(anizip_mapping_has_canonical_episode_numbers(&resolved));
    assert_eq!(resolved.episodes[0].absolute_episode_number, Some(13));
    assert_eq!(request_count(&requests, "metadata-only-refresh"), 1);
    let persisted: String = sqlx::query_scalar(
        "SELECT mapping_json FROM anizip_mapping_cache \
         WHERE anilist_id = 'metadata-only-refresh'",
    )
    .fetch_one(&database.pool)
    .await?;
    let persisted: AniZipMapping = serde_json::from_str(&persisted)?;
    assert!(anizip_mapping_has_canonical_episode_numbers(&persisted));
    Ok(())
}

#[tokio::test]
async fn alm2_first_scan_uses_canonical_season_seed_to_map_absolute_13() -> Result<()> {
    let database = database().await?;
    let media_dir = tempdir()?;
    let path = media_dir
        .path()
        .join("Tokyo Ghoul Root A - 13.mkv")
        .to_string_lossy()
        .to_string();
    std::fs::write(&path, b"fixture")?;
    let (base_url, _online, requests, shutdown) = start_anizip_fixture(true).await?;
    let config = linker_config(base_url);
    let linker = LinkerService::new(config.clone())?;
    let (pipeline, calls) = classifier_pipeline(vec![(
        path.clone(),
        absolute_hint("Tokyo Ghoul Root A", 13),
        absolute_candidate("Tokyo Ghoul Root A", "tokyo-ghoul-root-a", 2, 13),
    )]);

    run_full_scan_with_classifier(
        &database.pool,
        None,
        Some(&linker),
        Some(&config),
        None,
        &pipeline,
        vec![scan_candidate(&path, None, None)],
        false,
        false,
        false,
    )
    .await?;
    let _ = shutdown.send(());

    assert_eq!(
        linked_numbers(&database.pool, &path).await?,
        Some((2, 1, Some(13)))
    );
    assert_eq!(
        disposition(&database.pool, &path).await?.as_deref(),
        Some("applied")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(request_count(&requests, "tokyo-ghoul-root-a"), 1);
    let hydrated: (String, String, String, i64) = sqlx::query_as(
        "SELECT COALESCE(sr.external_tvdb_series, ''), \
                COALESCE(s.external_anilist, ''), COALESCE(s.title, ''), \
                (SELECT COUNT(*) FROM anime_episode_meta aem WHERE aem.season_id = s.id) \
         FROM series sr \
         INNER JOIN seasons s ON s.series_id = sr.id \
         WHERE s.season_number = 2 LIMIT 1",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(hydrated.0, "305014");
    assert_eq!(hydrated.1, "tokyo-ghoul-root-a");
    assert_eq!(hydrated.2, SERIES_TITLE);
    assert_eq!(hydrated.3, 2);
    Ok(())
}

#[tokio::test]
async fn alm2_root_anilist_id_without_known_season_maps_absolute_on_first_scan() -> Result<()> {
    let database = database().await?;
    let media_dir = tempdir()?;
    let path = media_dir
        .path()
        .join("Tokyo Ghoul Root A root-only - 13.mkv")
        .to_string_lossy()
        .to_string();
    std::fs::write(&path, b"fixture")?;
    let (base_url, _online, requests, shutdown) = start_anizip_fixture(true).await?;
    let config = linker_config(base_url);
    let linker = LinkerService::new(config.clone())?;
    let (pipeline, _calls) = classifier_pipeline(vec![(
        path.clone(),
        absolute_hint("Tokyo Ghoul Root A", 13),
        CandidateSpec {
            title: "Tokyo Ghoul Root A".to_string(),
            anilist_id: "tokyo-ghoul-root-a".to_string(),
            season: None,
            episode: None,
            absolute_episode: Some(13),
        },
    )]);

    run_full_scan_with_classifier(
        &database.pool,
        None,
        Some(&linker),
        Some(&config),
        None,
        &pipeline,
        vec![scan_candidate(&path, None, None)],
        false,
        false,
        false,
    )
    .await?;
    let _ = shutdown.send(());

    assert_eq!(
        linked_numbers(&database.pool, &path).await?,
        Some((2, 1, Some(13)))
    );
    assert_eq!(request_count(&requests, "tokyo-ghoul-root-a"), 1);
    Ok(())
}

#[tokio::test]
async fn alm2_multi_season_root_mapping_scaffolds_without_poisoning_season_identity() -> Result<()>
{
    let database = database().await?;
    let media_dir = tempdir()?;
    let path = media_dir
        .path()
        .join("Root-Spanning Anime - 25.mkv")
        .to_string_lossy()
        .to_string();
    std::fs::write(&path, b"fixture")?;
    let (base_url, _online, requests, shutdown) = start_anizip_fixture(true).await?;
    let config = linker_config(base_url);
    let linker = LinkerService::new(config.clone())?;
    let (pipeline, _calls) = classifier_pipeline(vec![(
        path.clone(),
        absolute_hint("Root-Spanning Anime", 25),
        CandidateSpec {
            title: "Root-Spanning Anime".to_string(),
            anilist_id: "shared-retry-id".to_string(),
            season: None,
            episode: None,
            absolute_episode: Some(25),
        },
    )]);

    run_full_scan_with_classifier(
        &database.pool,
        None,
        Some(&linker),
        Some(&config),
        None,
        &pipeline,
        vec![scan_candidate_with_title(
            &path,
            "Root-Spanning Anime",
            None,
            None,
        )],
        false,
        false,
        false,
    )
    .await?;
    let _ = shutdown.send(());

    assert_eq!(
        linked_numbers(&database.pool, &path).await?,
        Some((3, 1, Some(25)))
    );
    let season_context: (String, String, i64) = sqlx::query_as(
        "SELECT COALESCE(s.external_anilist, ''), COALESCE(s.title, ''), \
                (SELECT COUNT(*) FROM anime_episode_meta aem WHERE aem.season_id = s.id) \
         FROM seasons s \
         INNER JOIN series sr ON sr.id = s.series_id \
         WHERE sr.external_anilist = 'shared-retry-id' AND s.season_number = 3 LIMIT 1",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(season_context.0, "");
    assert_eq!(season_context.1, "");
    assert_eq!(season_context.2, 1, "the mapping must still scaffold S03");
    assert_eq!(request_count(&requests, "shared-retry-id"), 1);
    Ok(())
}

#[tokio::test]
async fn alm2_fresh_persisted_map_resolves_new_file_without_anizip_request() -> Result<()> {
    let database = database().await?;
    let media_dir = tempdir()?;
    let first_path = media_dir
        .path()
        .join("Tokyo Ghoul Root A - 13.mkv")
        .to_string_lossy()
        .to_string();
    let later_path = media_dir
        .path()
        .join("Tokyo Ghoul Root A - 14.mkv")
        .to_string_lossy()
        .to_string();
    std::fs::write(&first_path, b"fixture-13")?;
    std::fs::write(&later_path, b"fixture-14")?;
    let (base_url, online, requests, shutdown) = start_anizip_fixture(true).await?;
    let config = linker_config(base_url);
    let linker = LinkerService::new(config.clone())?;
    let metadata = disabled_metadata_with_ttl()?;
    let (pipeline, _calls) = classifier_pipeline(vec![
        (
            first_path.clone(),
            absolute_hint("Tokyo Ghoul Root A", 13),
            absolute_candidate("Tokyo Ghoul Root A", "tokyo-ghoul-root-a", 2, 13),
        ),
        (
            later_path.clone(),
            absolute_hint("Tokyo Ghoul Root A", 14),
            absolute_candidate("Tokyo Ghoul Root A", "tokyo-ghoul-root-a", 2, 14),
        ),
    ]);

    run_full_scan_with_classifier(
        &database.pool,
        Some(&metadata),
        Some(&linker),
        Some(&config),
        None,
        &pipeline,
        vec![scan_candidate(&first_path, None, None)],
        false,
        false,
        false,
    )
    .await?;
    assert_eq!(request_count(&requests, "tokyo-ghoul-root-a"), 1);

    let persisted_mapping_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM episodes WHERE absolute_episode_number IN (13, 14)",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(persisted_mapping_rows, 2);
    let persisted_cache_json: String = sqlx::query_scalar(
        "SELECT mapping_json FROM anizip_mapping_cache \
         WHERE anilist_id = 'tokyo-ghoul-root-a' AND schema_version = 1",
    )
    .fetch_one(&database.pool)
    .await?;
    let persisted_cache: AniZipMapping = serde_json::from_str(&persisted_cache_json)?;
    assert!(
        persisted_cache
            .episodes
            .iter()
            .any(|episode| episode.absolute_episode_number == Some(14)
                && episode.season_number == Some(2)
                && episode.episode_number == Some(2)),
        "the full mapping, including episodes not yet linked to files, must be persisted"
    );
    sqlx::query(
        "DELETE FROM anime_episode_meta WHERE episode_number = 2 AND season_id IN (\
             SELECT id FROM seasons WHERE season_number = 2\
         )",
    )
    .execute(&database.pool)
    .await?;
    sqlx::query("DELETE FROM episodes WHERE absolute_episode_number = 14 AND has_file = FALSE")
        .execute(&database.pool)
        .await?;
    online.store(false, Ordering::SeqCst);

    run_full_scan_with_classifier(
        &database.pool,
        Some(&metadata),
        Some(&linker),
        Some(&config),
        None,
        &pipeline,
        vec![scan_candidate(&later_path, None, None)],
        false,
        false,
        false,
    )
    .await?;
    let _ = shutdown.send(());

    assert_eq!(
        linked_numbers(&database.pool, &later_path).await?,
        Some((2, 2, Some(14)))
    );
    assert_eq!(
        request_count(&requests, "tokyo-ghoul-root-a"),
        1,
        "fresh persisted ani.zip data must populate the in-memory map without refetching"
    );
    let scaffolded_meta: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM anime_episode_meta aem \
         INNER JOIN seasons s ON s.id = aem.season_id \
         WHERE s.season_number = 2 AND aem.episode_number = 2",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        scaffolded_meta, 1,
        "persisted full mappings must hydrate scaffolds without a successful remote response"
    );
    Ok(())
}

#[tokio::test]
async fn alm2_multi_season_and_long_running_absolute_numbers_map_consistently() -> Result<()> {
    let database = database().await?;
    let media_dir = tempdir()?;
    let fixtures = [
        ("Long Anime - 1.mkv", 1, 1, "long-s1"),
        ("Long Anime - 13.mkv", 13, 2, "long-s2"),
        ("Long Anime - 101.mkv", 101, 9, "long-s9"),
    ];
    let mut classifier_fixtures = Vec::new();
    let mut scan_candidates = Vec::new();
    let mut paths = Vec::new();
    for (name, absolute, season, anilist_id) in fixtures {
        let path = media_dir.path().join(name).to_string_lossy().to_string();
        std::fs::write(&path, format!("fixture-{absolute}"))?;
        classifier_fixtures.push((
            path.clone(),
            absolute_hint("Long Anime", absolute),
            absolute_candidate("Long Anime", anilist_id, season, absolute),
        ));
        scan_candidates.push(scan_candidate(&path, None, None));
        paths.push(path);
    }
    let (base_url, _online, requests, shutdown) = start_anizip_fixture(true).await?;
    let config = linker_config(base_url);
    let linker = LinkerService::new(config.clone())?;
    let (pipeline, _calls) = classifier_pipeline(classifier_fixtures);

    run_full_scan_with_classifier(
        &database.pool,
        None,
        Some(&linker),
        Some(&config),
        None,
        &pipeline,
        scan_candidates,
        false,
        false,
        false,
    )
    .await?;
    let _ = shutdown.send(());

    assert_eq!(
        linked_numbers(&database.pool, &paths[0]).await?,
        Some((1, 1, Some(1)))
    );
    assert_eq!(
        linked_numbers(&database.pool, &paths[1]).await?,
        Some((2, 1, Some(13)))
    );
    assert_eq!(
        linked_numbers(&database.pool, &paths[2]).await?,
        Some((9, 5, Some(101)))
    );
    for anilist_id in ["long-s1", "long-s2", "long-s9"] {
        assert_eq!(request_count(&requests, anilist_id), 1);
    }
    Ok(())
}

#[tokio::test]
async fn alm2_metadata_outage_keeps_explicit_sxxeyy_and_never_invents_s01e01() -> Result<()> {
    let database = database().await?;
    let media_dir = tempdir()?;
    let path = media_dir
        .path()
        .join("Tokyo Ghoul Explicit S04E03.mkv")
        .to_string_lossy()
        .to_string();
    std::fs::write(&path, b"fixture")?;
    let (base_url, _online, _requests, shutdown) = start_anizip_fixture(false).await?;
    let config = linker_config(base_url);
    let linker = LinkerService::new(config.clone())?;
    let (pipeline, _calls) = classifier_pipeline(vec![(
        path.clone(),
        explicit_hint("Tokyo Ghoul", 4, 3),
        CandidateSpec {
            title: "Tokyo Ghoul".to_string(),
            anilist_id: "outage-explicit".to_string(),
            season: Some(4),
            episode: Some(3),
            absolute_episode: None,
        },
    )]);

    run_full_scan_with_classifier(
        &database.pool,
        None,
        Some(&linker),
        Some(&config),
        None,
        &pipeline,
        vec![scan_candidate(&path, Some(4), Some(3))],
        false,
        false,
        false,
    )
    .await?;
    let _ = shutdown.send(());

    assert_eq!(
        linked_numbers(&database.pool, &path).await?,
        Some((4, 3, None))
    );
    let s01e01_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM episodes WHERE season_number = 1 AND episode_number = 1",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(s01e01_count, 0);
    Ok(())
}

#[tokio::test]
async fn alm2_absolute_only_outage_retries_and_deduplicates_fetches_per_anilist_id() -> Result<()> {
    let database = database().await?;
    let media_dir = tempdir()?;
    let path_13 = media_dir
        .path()
        .join("Retry Anime - 13.mkv")
        .to_string_lossy()
        .to_string();
    let path_25 = media_dir
        .path()
        .join("Retry Anime - 25.mkv")
        .to_string_lossy()
        .to_string();
    std::fs::write(&path_13, b"fixture-13")?;
    std::fs::write(&path_25, b"fixture-25")?;
    let (base_url, online, requests, shutdown) = start_anizip_fixture(false).await?;
    let config = linker_config(base_url);
    let linker = LinkerService::new(config.clone())?;
    let (pipeline, calls) = classifier_pipeline(vec![
        (
            path_13.clone(),
            absolute_hint("Retry Anime", 13),
            absolute_candidate("Retry Anime", "shared-retry-id", 2, 13),
        ),
        (
            path_25.clone(),
            absolute_hint("Retry Anime Sequel", 25),
            absolute_candidate("Retry Anime Sequel", "shared-retry-id", 3, 25),
        ),
    ]);
    let candidates = || {
        vec![
            scan_candidate_with_title(&path_13, "Retry Anime", None, None),
            scan_candidate_with_title(&path_25, "Retry Anime Sequel", None, None),
        ]
    };

    run_full_scan_with_classifier(
        &database.pool,
        None,
        Some(&linker),
        Some(&config),
        None,
        &pipeline,
        candidates(),
        false,
        false,
        false,
    )
    .await?;

    assert_eq!(linked_numbers(&database.pool, &path_13).await?, None);
    assert_eq!(linked_numbers(&database.pool, &path_25).await?, None);
    assert_eq!(
        disposition(&database.pool, &path_13).await?.as_deref(),
        Some("unresolved")
    );
    assert_eq!(
        disposition(&database.pool, &path_25).await?.as_deref(),
        Some("unresolved")
    );
    assert_eq!(request_count(&requests, "shared-retry-id"), 1);
    let s01e01_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM episodes WHERE season_number = 1 AND episode_number = 1",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(s01e01_count, 0);

    online.store(true, Ordering::SeqCst);
    run_full_scan_with_classifier(
        &database.pool,
        None,
        Some(&linker),
        Some(&config),
        None,
        &pipeline,
        candidates(),
        false,
        false,
        false,
    )
    .await?;
    let _ = shutdown.send(());

    assert_eq!(
        linked_numbers(&database.pool, &path_13).await?,
        Some((2, 1, Some(13)))
    );
    assert_eq!(
        linked_numbers(&database.pool, &path_25).await?,
        Some((3, 1, Some(25)))
    );
    assert_eq!(
        disposition(&database.pool, &path_13).await?.as_deref(),
        Some("applied")
    );
    assert_eq!(
        disposition(&database.pool, &path_25).await?.as_deref(),
        Some("applied")
    );
    assert_eq!(request_count(&requests, "shared-retry-id"), 2);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        4,
        "both unresolved files must retry"
    );
    Ok(())
}
