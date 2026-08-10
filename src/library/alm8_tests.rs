use super::*;

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;
use tempfile::tempdir;

use crate::{
    anime_matching::{
        ANIME_MATCH_SCHEMA_VERSION, AnimeCandidateMatch, AnimeMatchAudioProfile, AnimeMatchEngine,
        AnimeMatchEngineOutput, AnimeMatchRequest, AnimeMatchResponse, AnimeMatchRuntimeProvenance,
    },
    config::DatabaseConfig,
    db::Database,
    extensions::{ExternalIds, FileDescriptor, MediaFileCandidate, MediaIdentity},
};

#[derive(Clone)]
struct LibraryMatchEngine {
    calls: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<AnimeMatchRequest>>>,
}

#[async_trait]
impl AnimeMatchEngine for LibraryMatchEngine {
    async fn match_candidates(&self, request: AnimeMatchRequest) -> Result<AnimeMatchResponse> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests
            .lock()
            .expect("library model request lock poisoned")
            .push(request);
        Ok(AnimeMatchResponse {
            schema_version: ANIME_MATCH_SCHEMA_VERSION,
            matches: vec![AnimeCandidateMatch {
                candidate_key: "candidate-0".to_string(),
                matched_target_keys: vec!["S02E01".to_string()],
                audio_profile: AnimeMatchAudioProfile::DualAudio,
                selected_file_keys: Some(vec!["candidate-0-file-0".to_string()]),
            }],
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum ScriptedLibraryReply {
    FirstWantedTarget,
    Empty,
    UnknownTarget,
    AllWantedTargets,
}

#[derive(Clone)]
struct ScriptedLibraryMatchEngine {
    calls: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<AnimeMatchRequest>>>,
    reply: ScriptedLibraryReply,
    runtime: Option<AnimeMatchRuntimeProvenance>,
}

impl ScriptedLibraryMatchEngine {
    fn response(&self, request: &AnimeMatchRequest) -> AnimeMatchResponse {
        let target_keys = match self.reply {
            ScriptedLibraryReply::FirstWantedTarget => request
                .target
                .wanted_target_keys
                .first()
                .cloned()
                .into_iter()
                .collect(),
            ScriptedLibraryReply::Empty => Vec::new(),
            ScriptedLibraryReply::UnknownTarget => vec!["S99E99".to_string()],
            ScriptedLibraryReply::AllWantedTargets => request.target.wanted_target_keys.clone(),
        };
        let matches = if matches!(self.reply, ScriptedLibraryReply::Empty) {
            Vec::new()
        } else {
            vec![AnimeCandidateMatch {
                candidate_key: "candidate-0".to_string(),
                matched_target_keys: target_keys,
                audio_profile: AnimeMatchAudioProfile::DualAudio,
                selected_file_keys: Some(vec!["candidate-0-file-0".to_string()]),
            }]
        };
        AnimeMatchResponse {
            schema_version: ANIME_MATCH_SCHEMA_VERSION,
            matches,
        }
    }

    fn record_and_respond(&self, request: AnimeMatchRequest) -> AnimeMatchEngineOutput {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let response = self.response(&request);
        self.requests
            .lock()
            .expect("scripted library model request lock poisoned")
            .push(request);
        AnimeMatchEngineOutput {
            response,
            runtime: self.runtime.clone(),
        }
    }
}

#[async_trait]
impl AnimeMatchEngine for ScriptedLibraryMatchEngine {
    async fn match_candidates(&self, request: AnimeMatchRequest) -> Result<AnimeMatchResponse> {
        Ok(self.record_and_respond(request).response)
    }

    async fn match_candidates_with_provenance(
        &self,
        request: AnimeMatchRequest,
    ) -> Result<AnimeMatchEngineOutput> {
        Ok(self.record_and_respond(request))
    }
}

async fn alm8_database() -> Result<Database> {
    let database = Database::connect(&DatabaseConfig {
        url: "sqlite::memory:?cache=shared".to_string(),
        max_connections: 1,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    Ok(database)
}

fn tokyo_ghoul_root_a_mapping() -> AniZipMapping {
    AniZipMapping {
        ids: ExternalIds {
            anilist: Some("1002".to_string()),
            tvdb_series: Some("305014".to_string()),
            ..Default::default()
        },
        episodes: vec![AniZipEpisodeRecord {
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

fn tokyo_ghoul_root_a_two_episode_mapping() -> AniZipMapping {
    let mut mapping = tokyo_ghoul_root_a_mapping();
    mapping.episodes.push(AniZipEpisodeRecord {
        season_number: Some(2),
        episode_number: Some(2),
        absolute_episode_number: Some(14),
        episode_label: Some("14".to_string()),
        mainline_episode_number: Some(14),
        title: Some("Dancing Flowers".to_string()),
        overview: None,
        runtime_minutes: Some(24),
        image: None,
        tvdb_id: Some("2014".to_string()),
        anidb_eid: Some("3014".to_string()),
        raw: json!({
            "episode": "14",
            "seasonNumber": 2,
            "episodeNumber": 2,
            "absoluteEpisodeNumber": 14
        }),
    });
    mapping
}

fn tokyo_ghoul_ova_mapping() -> AniZipMapping {
    AniZipMapping {
        ids: ExternalIds {
            anilist: Some("1003".to_string()),
            tvdb_series: Some("305014".to_string()),
            ..Default::default()
        },
        episodes: vec![AniZipEpisodeRecord {
            season_number: Some(0),
            episode_number: Some(1),
            absolute_episode_number: None,
            episode_label: Some("S1".to_string()),
            mainline_episode_number: None,
            title: Some("Jack".to_string()),
            overview: None,
            runtime_minutes: Some(30),
            image: None,
            tvdb_id: Some("2001".to_string()),
            anidb_eid: Some("3001".to_string()),
            raw: json!({
                "episode": "S1",
                "seasonNumber": 0,
                "episodeNumber": 1
            }),
        }],
        images: Vec::new(),
        titles: HashMap::from([
            ("en".to_string(), "Tokyo Ghoul: JACK".to_string()),
            ("x-jat".to_string(), "Tokyo Ghoul: JACK".to_string()),
            (
                "ja".to_string(),
                "東京喰種トーキョーグール JACK".to_string(),
            ),
        ]),
    }
}

fn anime_candidate(path: &str, season: Option<i32>, episode: Option<i32>) -> MediaFileCandidate {
    anime_candidate_with_anilist(path, season, episode, "1002")
}

fn anime_candidate_with_anilist(
    path: &str,
    season: Option<i32>,
    episode: Option<i32>,
    anilist_id: &str,
) -> MediaFileCandidate {
    MediaFileCandidate {
        identity: MediaIdentity {
            r#type: MediaType::Anime,
            external_ids: ExternalIds {
                anilist: Some(anilist_id.to_string()),
                ..Default::default()
            },
            title: "Tokyo Ghoul".to_string(),
            year: Some(2014),
            season,
            episode,
        },
        files: vec![FileDescriptor {
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

async fn linked_episode(pool: &AnyPool, path: &str) -> Result<Option<(i64, i64, Option<i64>)>> {
    Ok(sqlx::query_as(
        "SELECT e.season_number, e.episode_number, e.absolute_episode_number \
         FROM episode_files ef \
         JOIN episodes e ON e.id = ef.episode_id \
         JOIN media_files mf ON mf.id = ef.media_file_id \
         WHERE mf.path = $1",
    )
    .bind(path)
    .fetch_optional(pool)
    .await?)
}

async fn scan_with_matcher(
    database: &Database,
    matcher: &AnimeMatchingService,
    candidate: MediaFileCandidate,
) -> Result<()> {
    scan_candidates_with_matcher(database, matcher, vec![candidate]).await
}

async fn scan_candidates_with_matcher(
    database: &Database,
    matcher: &AnimeMatchingService,
    candidates: Vec<MediaFileCandidate>,
) -> Result<()> {
    let classifier = ClassifierPipeline::new();
    run_full_scan_with_classifier_and_anime_matching(
        &database.pool,
        None,
        None,
        None,
        None,
        &classifier,
        matcher,
        candidates,
        false,
        false,
        false,
    )
    .await
}

async fn classification_state(
    pool: &AnyPool,
    path: &str,
) -> Result<(String, String, i64, String, String)> {
    Ok(sqlx::query_as(
        "SELECT crs.disposition, crs.candidates_json, crs.applied_identity_version, \
                crs.applied_identity_evidence_json, crs.anime_match_assist_json \
         FROM classifier_resolution_state crs \
         JOIN media_files mf ON mf.id = crs.media_file_id WHERE mf.path = $1",
    )
    .bind(path)
    .fetch_one(pool)
    .await?)
}

#[tokio::test]
async fn alm8_unresolved_library_file_uses_model_and_links_one_canonical_episode() -> Result<()> {
    let database = alm8_database().await?;
    persist_cached_anizip_mapping(&database.pool, "1002", &tokyo_ghoul_root_a_mapping()).await?;
    let dir = tempdir()?;
    let path = dir
        .path()
        .join("[Group] Tokyo Ghoul Root A - 01 [Dual Audio].mkv")
        .to_string_lossy()
        .to_string();
    std::fs::write(&path, b"alm8-model-fixture")?;
    let calls = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let matcher = AnimeMatchingService::with_engine(Arc::new(ScriptedLibraryMatchEngine {
        calls: calls.clone(),
        requests: requests.clone(),
        reply: ScriptedLibraryReply::FirstWantedTarget,
        runtime: Some(AnimeMatchRuntimeProvenance {
            bundle_version: "2026.08.1".to_string(),
            model_id: "qwen3-4b-instruct-2507".to_string(),
            model_revision: "elixir-q4km-r1".to_string(),
            worker_revision: "llama-server-r1".to_string(),
            backend: "metal".to_string(),
            profile_fingerprint: "alm8-profile-fingerprint".to_string(),
            prompt_revision: "anime-match-v1".to_string(),
            protocol_version: 1,
        }),
    }));

    scan_with_matcher(&database, &matcher, anime_candidate(&path, None, None)).await?;

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let requests = requests.lock().expect("request lock poisoned");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].target.wanted_target_keys, vec!["S02E01"]);
    assert_eq!(requests[0].candidates.len(), 1);
    assert_eq!(
        requests[0].candidates[0].files[0].path,
        "[Group] Tokyo Ghoul Root A - 01 [Dual Audio].mkv"
    );
    drop(requests);
    assert_eq!(
        linked_episode(&database.pool, &path).await?,
        Some((2, 1, Some(13)))
    );
    let link_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM episode_files ef \
         JOIN media_files mf ON mf.id = ef.media_file_id WHERE mf.path = $1",
    )
    .bind(&path)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(link_count, 1);
    let state = classification_state(&database.pool, &path).await?;
    assert_eq!(state.0, "applied");
    let evidence: serde_json::Value = serde_json::from_str(&state.1)?;
    assert_eq!(evidence["animeMatchAssist"]["source"], "local_model");
    assert_eq!(evidence["animeMatchAssist"]["result"], "matched");
    assert_eq!(
        evidence["animeMatchAssist"]["runtime"]["modelId"],
        "qwen3-4b-instruct-2507"
    );
    assert_eq!(
        evidence["animeMatchAssist"]["runtime"]["modelRevision"],
        "elixir-q4km-r1"
    );
    assert_eq!(state.2, 2);
    let applied_identity: serde_json::Value = serde_json::from_str(&state.3)?;
    assert_eq!(applied_identity["schemaVersion"], 2);
    assert_eq!(applied_identity["origin"], "local_model");
    assert_eq!(applied_identity["acceptedNumbers"]["seasonNumber"], 2);
    assert_eq!(applied_identity["acceptedNumbers"]["episodeNumber"], 1);
    assert_eq!(
        applied_identity["acceptedNumbers"]["absoluteEpisodeNumber"],
        13
    );
    assert_eq!(
        applied_identity["causalIdentityRows"],
        json!({ "series": [], "seasons": [], "episodes": [] })
    );
    let assist: serde_json::Value = serde_json::from_str(&state.4)?;
    assert_eq!(assist["animeMatchAssist"]["source"], "local_model");
    assert_eq!(
        assist["animeMatchAssist"]["runtime"]["bundleVersion"],
        "2026.08.1"
    );
    assert_eq!(
        assist["animeMatchAssist"]["runtime"]["profileFingerprint"],
        "alm8-profile-fingerprint"
    );

    let season_source: String = sqlx::query_scalar(
        "SELECT sei.source FROM season_external_ids sei \
         JOIN seasons s ON s.id = sei.season_id \
         JOIN episodes e ON e.season_id = s.id \
         JOIN episode_files ef ON ef.episode_id = e.id \
         JOIN media_files mf ON mf.id = ef.media_file_id \
         WHERE mf.path = $1 AND sei.provider = 'anilist' AND sei.external_id = '1002' \
         LIMIT 1",
    )
    .bind(&path)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(season_source, "anime_match");
    let series_source: String = sqlx::query_scalar(
        "SELECT sei.source FROM series_external_ids sei \
         JOIN media_files mf ON mf.media_item_id = sei.series_id \
         WHERE mf.path = $1 AND sei.provider = 'anilist' AND sei.external_id = '1002' \
         LIMIT 1",
    )
    .bind(&path)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(series_source, "anime_match");
    Ok(())
}

#[tokio::test]
async fn alm8_model_promotion_merges_into_existing_canonical_series() -> Result<()> {
    let database = alm8_database().await?;
    persist_cached_anizip_mapping(&database.pool, "1002", &tokyo_ghoul_root_a_mapping()).await?;
    let canonical_series_id = Uuid::new_v4();
    let canonical_ids = ExternalIds {
        anilist: Some("1002".to_string()),
        tvdb_series: Some("305014".to_string()),
        ..Default::default()
    };
    sqlx::query::<sqlx::Any>(
        "INSERT INTO media_items (id, type, external_ids, title, year) \
         VALUES ($1, 'anime', $2, 'Tokyo Ghoul', 2014)",
    )
    .bind(canonical_series_id.to_string())
    .bind(serde_json::to_string(&canonical_ids)?)
    .execute(&database.pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO series \
         (id, title, year, library_type, external_tvdb_series, external_anilist) \
         VALUES ($1, 'Tokyo Ghoul', 2014, 'anime', '305014', '1002')",
    )
    .bind(canonical_series_id.to_string())
    .execute(&database.pool)
    .await?;

    let dir = tempdir()?;
    let path = dir
        .path()
        .join("[Group] Tokyo Ghoul Root A - 01 [Dual Audio].mkv")
        .to_string_lossy()
        .to_string();
    std::fs::write(&path, b"alm8-model-promotion")?;
    let calls = Arc::new(AtomicUsize::new(0));
    let matcher = AnimeMatchingService::with_engine(Arc::new(ScriptedLibraryMatchEngine {
        calls: calls.clone(),
        requests: Arc::new(Mutex::new(Vec::new())),
        reply: ScriptedLibraryReply::FirstWantedTarget,
        runtime: None,
    }));
    let mut candidate = anime_candidate(&path, None, None);
    candidate.identity.title = "Tokyo Ghoul Root A".to_string();

    scan_with_matcher(&database, &matcher, candidate).await?;

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let series_rows: Vec<(String, String)> =
        sqlx::query_as("SELECT id, title FROM series ORDER BY id")
            .fetch_all(&database.pool)
            .await?;
    assert_eq!(
        series_rows,
        vec![(canonical_series_id.to_string(), "Tokyo Ghoul".to_string())]
    );
    let file_owner: String =
        sqlx::query_scalar("SELECT media_item_id FROM media_files WHERE path = $1")
            .bind(&path)
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(file_owner, canonical_series_id.to_string());
    assert_eq!(
        linked_episode(&database.pool, &path).await?,
        Some((2, 1, Some(13)))
    );
    Ok(())
}

#[tokio::test]
async fn alm8_model_unavailable_preserves_unattached_retryable_fallback() -> Result<()> {
    let database = alm8_database().await?;
    persist_cached_anizip_mapping(&database.pool, "1002", &tokyo_ghoul_root_a_mapping()).await?;
    let dir = tempdir()?;
    let path = dir
        .path()
        .join("Tokyo Ghoul Root A Mystery 01.mkv")
        .to_string_lossy()
        .to_string();
    std::fs::write(&path, b"alm8-fallback-fixture")?;

    scan_with_matcher(
        &database,
        &AnimeMatchingService::disabled(),
        anime_candidate(&path, None, None),
    )
    .await?;

    assert_eq!(linked_episode(&database.pool, &path).await?, None);
    assert!(std::path::Path::new(&path).is_file());
    let state: (String, String) = sqlx::query_as(
        "SELECT disposition, candidates_json FROM classifier_resolution_state crs \
         JOIN media_files mf ON mf.id = crs.media_file_id WHERE mf.path = $1",
    )
    .bind(&path)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(state.0, "unresolved");
    let evidence: serde_json::Value = serde_json::from_str(&state.1)?;
    assert_eq!(
        evidence["animeMatchAssist"]["source"],
        "deterministic_fallback"
    );
    assert_eq!(evidence["animeMatchAssist"]["reason"], "engine_unavailable");
    let mutation_counts: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM seasons), (SELECT COUNT(*) FROM episodes), \
                (SELECT COUNT(*) FROM episode_files), \
                (SELECT COUNT(*) FROM series_external_ids), \
                (SELECT COUNT(*) FROM season_external_ids)",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(mutation_counts, (0, 0, 0, 0, 0));
    let placeholder: (Option<String>, Option<String>, String) = sqlx::query_as(
        "SELECT external_tvdb_series, external_anilist, metadata_json FROM series LIMIT 1",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(placeholder.0, None);
    assert_eq!(placeholder.1, None);
    assert_eq!(placeholder.2, r#"{"classifierPlaceholder":true}"#);
    Ok(())
}

async fn assert_scripted_model_fallback(
    reply: ScriptedLibraryReply,
    expected_reason: &str,
    mapping: AniZipMapping,
    filename: &str,
    expected_wanted_target_count: usize,
) -> Result<()> {
    let database = alm8_database().await?;
    let anilist_id = mapping
        .ids
        .anilist
        .clone()
        .expect("scripted fallback fixture AniList id");
    persist_cached_anizip_mapping(&database.pool, &anilist_id, &mapping).await?;
    let dir = tempdir()?;
    let path = dir.path().join(filename).to_string_lossy().to_string();
    std::fs::write(&path, b"alm8-scripted-fallback")?;
    let calls = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let matcher = AnimeMatchingService::with_engine(Arc::new(ScriptedLibraryMatchEngine {
        calls: calls.clone(),
        requests: requests.clone(),
        reply,
        runtime: None,
    }));

    scan_with_matcher(
        &database,
        &matcher,
        anime_candidate_with_anilist(&path, None, None, &anilist_id),
    )
    .await?;

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let requests = requests.lock().expect("scripted request lock poisoned");
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].target.wanted_target_keys.len(),
        expected_wanted_target_count
    );
    drop(requests);
    assert_eq!(linked_episode(&database.pool, &path).await?, None);
    assert!(std::path::Path::new(&path).is_file());

    let state: (String, String, i64, Option<String>, String) = sqlx::query_as(
        "SELECT crs.disposition, crs.candidates_json, crs.applied_identity_version, \
                crs.applied_identity_evidence_json, crs.anime_match_assist_json \
         FROM classifier_resolution_state crs \
         JOIN media_files mf ON mf.id = crs.media_file_id WHERE mf.path = $1",
    )
    .bind(&path)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(state.0, "unresolved");
    assert_eq!(state.2, 0);
    assert_eq!(state.3, None);
    let candidates: serde_json::Value = serde_json::from_str(&state.1)?;
    assert_eq!(
        candidates["animeMatchAssist"]["source"],
        "deterministic_fallback"
    );
    assert_eq!(candidates["animeMatchAssist"]["reason"], expected_reason);
    let assist: serde_json::Value = serde_json::from_str(&state.4)?;
    assert_eq!(
        assist["animeMatchAssist"]["source"],
        "deterministic_fallback"
    );
    assert_eq!(assist["animeMatchAssist"]["reason"], expected_reason);
    let mutation_counts: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM seasons), (SELECT COUNT(*) FROM episodes), \
                (SELECT COUNT(*) FROM episode_files), \
                (SELECT COUNT(*) FROM series_external_ids), \
                (SELECT COUNT(*) FROM season_external_ids)",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(mutation_counts, (0, 0, 0, 0, 0));
    Ok(())
}

#[tokio::test]
async fn alm8_invalid_model_response_is_retryable_and_unattached() -> Result<()> {
    assert_scripted_model_fallback(
        ScriptedLibraryReply::UnknownTarget,
        "invalid_model_response",
        tokyo_ghoul_root_a_mapping(),
        "Tokyo Ghoul Root A Mystery 01.mkv",
        1,
    )
    .await
}

#[tokio::test]
async fn alm8_empty_model_response_is_retryable_and_unattached() -> Result<()> {
    assert_scripted_model_fallback(
        ScriptedLibraryReply::Empty,
        "empty_model_matches",
        tokyo_ghoul_root_a_mapping(),
        "Tokyo Ghoul Root A Mystery 01.mkv",
        1,
    )
    .await
}

#[tokio::test]
async fn alm8_two_target_model_response_is_retryable_and_unattached() -> Result<()> {
    assert_scripted_model_fallback(
        ScriptedLibraryReply::AllWantedTargets,
        "coverage_validation_failed",
        tokyo_ghoul_root_a_two_episode_mapping(),
        "Tokyo Ghoul Root A Mystery.mkv",
        2,
    )
    .await
}

#[tokio::test]
async fn alm8_definitive_library_fast_path_never_calls_model() -> Result<()> {
    let database = alm8_database().await?;
    persist_cached_anizip_mapping(&database.pool, "1002", &tokyo_ghoul_root_a_mapping()).await?;
    let dir = tempdir()?;
    let path = dir
        .path()
        .join("Tokyo Ghoul S02E01.mkv")
        .to_string_lossy()
        .to_string();
    std::fs::write(&path, b"alm8-fast-path")?;
    let calls = Arc::new(AtomicUsize::new(0));
    let matcher = AnimeMatchingService::with_engine(Arc::new(LibraryMatchEngine {
        calls: calls.clone(),
        requests: Arc::new(Mutex::new(Vec::new())),
    }));

    scan_with_matcher(
        &database,
        &matcher,
        anime_candidate(&path, Some(2), Some(1)),
    )
    .await?;

    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        linked_episode(&database.pool, &path).await?,
        Some((2, 1, Some(13)))
    );
    Ok(())
}

#[tokio::test]
async fn alm8_mixed_scan_calls_model_only_for_unresolved_files() -> Result<()> {
    let database = alm8_database().await?;
    persist_cached_anizip_mapping(&database.pool, "1002", &tokyo_ghoul_root_a_mapping()).await?;
    let dir = tempdir()?;
    let definitive_path = dir
        .path()
        .join("Tokyo Ghoul S02E01.mkv")
        .to_string_lossy()
        .to_string();
    let unresolved_path = dir
        .path()
        .join("[Group] Tokyo Ghoul Root A - 01 [Dual Audio].mkv")
        .to_string_lossy()
        .to_string();
    std::fs::write(&definitive_path, b"alm8-mixed-definitive")?;
    std::fs::write(&unresolved_path, b"alm8-mixed-unresolved")?;
    let calls = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let matcher = AnimeMatchingService::with_engine(Arc::new(LibraryMatchEngine {
        calls: calls.clone(),
        requests: requests.clone(),
    }));

    scan_candidates_with_matcher(
        &database,
        &matcher,
        vec![
            anime_candidate(&definitive_path, Some(2), Some(1)),
            anime_candidate(&unresolved_path, None, None),
        ],
    )
    .await?;

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let requests = requests.lock().expect("mixed request lock poisoned");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].candidates.len(), 1);
    assert_eq!(
        requests[0].candidates[0].files[0].path,
        "[Group] Tokyo Ghoul Root A - 01 [Dual Audio].mkv"
    );
    drop(requests);
    assert_eq!(
        linked_episode(&database.pool, &definitive_path).await?,
        Some((2, 1, Some(13)))
    );
    assert_eq!(
        linked_episode(&database.pool, &unresolved_path).await?,
        Some((2, 1, Some(13)))
    );
    Ok(())
}

#[tokio::test]
async fn alm8_successful_model_match_is_idempotent_on_rescan() -> Result<()> {
    let database = alm8_database().await?;
    persist_cached_anizip_mapping(&database.pool, "1002", &tokyo_ghoul_root_a_mapping()).await?;
    let dir = tempdir()?;
    let path = dir
        .path()
        .join("[Group] Tokyo Ghoul Root A - 01 [Dual Audio].mkv")
        .to_string_lossy()
        .to_string();
    std::fs::write(&path, b"alm8-idempotent-rescan")?;
    let calls = Arc::new(AtomicUsize::new(0));
    let matcher = AnimeMatchingService::with_engine(Arc::new(LibraryMatchEngine {
        calls: calls.clone(),
        requests: Arc::new(Mutex::new(Vec::new())),
    }));

    scan_with_matcher(&database, &matcher, anime_candidate(&path, None, None)).await?;
    let first_state = classification_state(&database.pool, &path).await?;
    scan_with_matcher(&database, &matcher, anime_candidate(&path, None, None)).await?;
    let second_state = classification_state(&database.pool, &path).await?;

    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the persisted canonical link must become the deterministic rescan fast path"
    );
    assert_eq!(first_state, second_state);
    assert_eq!(
        linked_episode(&database.pool, &path).await?,
        Some((2, 1, Some(13)))
    );
    let link_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM episode_files ef \
         JOIN media_files mf ON mf.id = ef.media_file_id WHERE mf.path = $1",
    )
    .bind(&path)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(link_count, 1);
    Ok(())
}

#[tokio::test]
async fn alm8_completed_repair_fences_a_conflicting_stale_scan_relink() -> Result<()> {
    let database = alm8_database().await?;
    let series_id = Uuid::new_v4();
    let season_id = Uuid::new_v4();
    let repaired_episode_id = Uuid::new_v4();
    let stale_episode_id = Uuid::new_v4();
    let media_file_id = Uuid::new_v4();

    sqlx::query::<sqlx::Any>(
        "INSERT INTO media_items (id, type, external_ids, title, year) \
         VALUES ($1, 'anime', '{}', 'Tokyo Ghoul', 2014)",
    )
    .bind(series_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO series (id, title, year, library_type) \
         VALUES ($1, 'Tokyo Ghoul', 2014, 'anime')",
    )
    .bind(series_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO seasons (id, series_id, season_number) VALUES ($1, $2, 2)",
    )
    .bind(season_id.to_string())
    .bind(series_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO episodes \
         (id, series_id, season_id, season_number, episode_number, has_file) \
         VALUES ($1, $2, $3, 2, 1, TRUE), ($4, $2, $3, 2, 2, FALSE)",
    )
    .bind(repaired_episode_id.to_string())
    .bind(series_id.to_string())
    .bind(season_id.to_string())
    .bind(stale_episode_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO media_files (id, media_item_id, path, scan_state) \
         VALUES ($1, $2, '/media/alm8-stale-scan.mkv', 'ok')",
    )
    .bind(media_file_id.to_string())
    .bind(series_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "UPDATE media_files SET size_bytes = 1, hash = 'old-hash', container = 'avi', \
         video_codec = 'mpeg4', audio_codec = 'mp3' WHERE id = $1",
    )
    .bind(media_file_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO episode_files (episode_id, media_file_id) VALUES ($1, $2)",
    )
    .bind(repaired_episode_id.to_string())
    .bind(media_file_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO library_anime_repairs \
         (media_file_id, repair_version, status, attempt_count, repaired_link_count, \
          repaired_identity_count, reason, evidence_snapshot_json, completed_at) \
         VALUES ($1, $2, 'completed', 1, 1, 0, 'fixture', '{}', CURRENT_TIMESTAMP)",
    )
    .bind(media_file_id.to_string())
    .bind(ANIME_LIBRARY_REPAIR_VERSION)
    .execute(&database.pool)
    .await?;

    replace_episode_file_links(&database.pool, media_file_id, &[stale_episode_id]).await?;

    let linked_episode_ids: Vec<String> = sqlx::query_scalar(
        "SELECT episode_id FROM episode_files WHERE media_file_id = $1 ORDER BY episode_id",
    )
    .bind(media_file_id.to_string())
    .fetch_all(&database.pool)
    .await?;
    assert_eq!(linked_episode_ids, vec![repaired_episode_id.to_string()]);
    let file_owner: String =
        sqlx::query_scalar("SELECT media_item_id FROM media_files WHERE id = $1")
            .bind(media_file_id.to_string())
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(file_owner, series_id.to_string());
    let has_file: (i64, i64) = sqlx::query_as(
        "SELECT \
         (SELECT CASE WHEN has_file THEN 1 ELSE 0 END FROM episodes WHERE id = $1), \
         (SELECT CASE WHEN has_file THEN 1 ELSE 0 END FROM episodes WHERE id = $2)",
    )
    .bind(repaired_episode_id.to_string())
    .bind(stale_episode_id.to_string())
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(has_file, (1, 0));

    // Exercise the real scan boundary, not only the low-level relinker. A
    // later stale scan must not create movie/identity/scaffold rows or replace
    // classification after this repair version completed.
    let stale_movie_candidate = MediaFileCandidate {
        identity: MediaIdentity {
            r#type: MediaType::Movie,
            external_ids: ExternalIds {
                tmdb: Some("999999".to_string()),
                ..Default::default()
            },
            title: "Stale Tokyo Ghoul Movie Guess".to_string(),
            year: Some(2014),
            season: None,
            episode: None,
        },
        files: vec![FileDescriptor {
            path: "/media/alm8-stale-scan.mkv".to_string(),
            size_bytes: Some(4_096),
            hash: Some("replacement-hash".to_string()),
            container: Some("mkv".to_string()),
            video_codec: Some("h264".to_string()),
            audio_codec: Some("aac".to_string()),
        }],
        extension_metadata: HashMap::from([("replacementQuality".to_string(), json!("higher"))]),
        source_config_id: None,
    };
    scan_with_matcher(
        &database,
        &AnimeMatchingService::disabled(),
        stale_movie_candidate,
    )
    .await?;

    let identity_counts: (i64, i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT \
         (SELECT COUNT(*) FROM series), \
         (SELECT COUNT(*) FROM seasons), \
         (SELECT COUNT(*) FROM episodes), \
         (SELECT COUNT(*) FROM movies), \
         (SELECT COUNT(*) FROM movie_files), \
         (SELECT COUNT(*) FROM classifier_resolution_state)",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(identity_counts, (1, 1, 2, 0, 0, 0));
    let technical_state: (i64, String, String, String, String, String, String) = sqlx::query_as(
        "SELECT size_bytes, hash, container, video_codec, audio_codec, \
                    extension_metadata, scan_state FROM media_files WHERE id = $1",
    )
    .bind(media_file_id.to_string())
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(technical_state.0, 4_096);
    assert_eq!(technical_state.1, "replacement-hash");
    assert_eq!(technical_state.2, "mkv");
    assert_eq!(technical_state.3, "h264");
    assert_eq!(technical_state.4, "aac");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&technical_state.5)?,
        json!({ "replacementQuality": "higher" })
    );
    assert_eq!(technical_state.6, "ok");
    let probe_status: String =
        sqlx::query_scalar("SELECT probe_status FROM media_file_probes WHERE media_file_id = $1")
            .bind(media_file_id.to_string())
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(probe_status, "probe_failed");
    let linked_episode_ids: Vec<String> = sqlx::query_scalar(
        "SELECT episode_id FROM episode_files WHERE media_file_id = $1 ORDER BY episode_id",
    )
    .bind(media_file_id.to_string())
    .fetch_all(&database.pool)
    .await?;
    assert_eq!(linked_episode_ids, vec![repaired_episode_id.to_string()]);
    Ok(())
}

#[tokio::test]
async fn alm8_file_scoped_override_cannot_bypass_a_sibling_repair_fence() -> Result<()> {
    let database = alm8_database().await?;
    let directory = tempdir()?;
    let override_path = directory
        .path()
        .join("manual-movie-override.mkv")
        .to_string_lossy()
        .to_string();
    let ordinary_path = directory
        .path()
        .join("ordinary-sibling.mkv")
        .to_string_lossy()
        .to_string();
    std::fs::write(&override_path, b"override")?;
    std::fs::write(&ordinary_path, b"ordinary")?;

    let series_id = Uuid::new_v4();
    let season_id = Uuid::new_v4();
    let override_episode_id = Uuid::new_v4();
    let ordinary_episode_id = Uuid::new_v4();
    let override_file_id = Uuid::new_v4();
    let ordinary_file_id = Uuid::new_v4();
    sqlx::query::<sqlx::Any>(
        "INSERT INTO media_items (id, type, external_ids, title, year) \
         VALUES ($1, 'anime', '{}', 'Mixed Repair Fence', 2014)",
    )
    .bind(series_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO series (id, title, year, library_type) \
         VALUES ($1, 'Mixed Repair Fence', 2014, 'anime')",
    )
    .bind(series_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO seasons (id, series_id, season_number) VALUES ($1, $2, 1)",
    )
    .bind(season_id.to_string())
    .bind(series_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO episodes \
         (id, series_id, season_id, season_number, episode_number, has_file) \
         VALUES ($1, $2, $3, 1, 1, TRUE), ($4, $2, $3, 1, 2, TRUE)",
    )
    .bind(override_episode_id.to_string())
    .bind(series_id.to_string())
    .bind(season_id.to_string())
    .bind(ordinary_episode_id.to_string())
    .execute(&database.pool)
    .await?;
    for (file_id, path, episode_id) in [
        (override_file_id, &override_path, override_episode_id),
        (ordinary_file_id, &ordinary_path, ordinary_episode_id),
    ] {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO media_files \
             (id, media_item_id, path, size_bytes, scan_state) \
             VALUES ($1, $2, $3, 1, 'ok')",
        )
        .bind(file_id.to_string())
        .bind(series_id.to_string())
        .bind(path)
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO episode_files (episode_id, media_file_id) VALUES ($1, $2)",
        )
        .bind(episode_id.to_string())
        .bind(file_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO library_anime_repairs \
             (media_file_id, repair_version, status, attempt_count, repaired_link_count, \
              repaired_identity_count, reason, evidence_snapshot_json, completed_at) \
             VALUES ($1, $2, 'completed', 1, 1, 0, 'fixture', '{}', CURRENT_TIMESTAMP)",
        )
        .bind(file_id.to_string())
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .execute(&database.pool)
        .await?;
    }

    let override_key = derive_override_key("movie", &override_path)
        .expect("movie override fixture must have a stable key");
    sqlx::query::<sqlx::Any>(
        "INSERT INTO classifier_overrides (id, library_type, normalized_key, imdb_id) \
         VALUES ($1, 'movie', $2, 'tt-mixed-override')",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(override_key)
    .execute(&database.pool)
    .await?;

    let descriptor = |path: &str, size_bytes: i64| FileDescriptor {
        path: path.to_string(),
        size_bytes: Some(size_bytes),
        hash: None,
        container: Some("mkv".to_string()),
        video_codec: Some("h264".to_string()),
        audio_codec: Some("aac".to_string()),
    };
    scan_with_matcher(
        &database,
        &AnimeMatchingService::disabled(),
        MediaFileCandidate {
            identity: MediaIdentity {
                r#type: MediaType::Movie,
                external_ids: ExternalIds {
                    tmdb: Some("mixed-override-movie".to_string()),
                    ..Default::default()
                },
                title: "Mixed Override Movie".to_string(),
                year: Some(2014),
                season: None,
                episode: None,
            },
            files: vec![descriptor(&override_path, 8), descriptor(&ordinary_path, 8)],
            extension_metadata: HashMap::new(),
            source_config_id: None,
        },
    )
    .await?;

    let override_links: (i64, i64) = sqlx::query_as(
        "SELECT \
         (SELECT COUNT(*) FROM movie_files WHERE media_file_id = $1), \
         (SELECT COUNT(*) FROM episode_files WHERE media_file_id = $1)",
    )
    .bind(override_file_id.to_string())
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(override_links, (1, 0));
    let ordinary_links: (i64, i64, i64) = sqlx::query_as(
        "SELECT \
         (SELECT COUNT(*) FROM movie_files WHERE media_file_id = $1), \
         (SELECT COUNT(*) FROM episode_files \
          WHERE media_file_id = $1 AND episode_id = $2), \
         (SELECT size_bytes FROM media_files WHERE id = $1)",
    )
    .bind(ordinary_file_id.to_string())
    .bind(ordinary_episode_id.to_string())
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(ordinary_links, (0, 1, 8));
    Ok(())
}

#[tokio::test]
async fn alm8_special_season_model_match_links_s00_without_normalizing_to_s01() -> Result<()> {
    let database = alm8_database().await?;
    persist_cached_anizip_mapping(&database.pool, "1003", &tokyo_ghoul_ova_mapping()).await?;
    let dir = tempdir()?;
    let path = dir
        .path()
        .join("[Group] Tokyo Ghoul JACK - 01 [Dual Audio].mkv")
        .to_string_lossy()
        .to_string();
    std::fs::write(&path, b"alm8-special-season")?;
    let calls = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let matcher = AnimeMatchingService::with_engine(Arc::new(ScriptedLibraryMatchEngine {
        calls: calls.clone(),
        requests: requests.clone(),
        reply: ScriptedLibraryReply::FirstWantedTarget,
        runtime: None,
    }));

    scan_with_matcher(
        &database,
        &matcher,
        anime_candidate_with_anilist(&path, None, None, "1003"),
    )
    .await?;

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let requests = requests.lock().expect("special request lock poisoned");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].target.wanted_target_keys, vec!["S00E01"]);
    drop(requests);
    assert_eq!(
        linked_episode(&database.pool, &path).await?,
        Some((0, 1, None))
    );
    Ok(())
}

async fn seed_authoritative_two_episode_pack(
    pool: &AnyPool,
    path: &str,
) -> Result<(Uuid, Uuid, Uuid)> {
    let series_id = Uuid::new_v4();
    let season_id = Uuid::new_v4();
    let episode_one_id = Uuid::new_v4();
    let episode_two_id = Uuid::new_v4();
    let media_file_id = Uuid::new_v4();
    let external_ids = serde_json::to_string(&ExternalIds {
        anilist: Some("1002".to_string()),
        tvdb_series: Some("305014".to_string()),
        ..Default::default()
    })?;

    sqlx::query::<sqlx::Any>(
        "INSERT INTO media_items (id, type, external_ids, title, year) \
         VALUES ($1, 'anime', $2, 'Tokyo Ghoul', 2014)",
    )
    .bind(series_id.to_string())
    .bind(external_ids)
    .execute(pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO series \
         (id, title, year, library_type, external_tvdb_series, external_anilist) \
         VALUES ($1, 'Tokyo Ghoul', 2014, 'anime', '305014', '1002')",
    )
    .bind(series_id.to_string())
    .execute(pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO seasons (id, series_id, season_number, external_anilist) \
         VALUES ($1, $2, 2, '1002')",
    )
    .bind(season_id.to_string())
    .bind(series_id.to_string())
    .execute(pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO episodes \
         (id, series_id, season_id, season_number, episode_number, \
          absolute_episode_number, has_file) \
         VALUES ($1, $2, $3, 2, 1, 13, TRUE), ($4, $2, $3, 2, 2, 14, TRUE)",
    )
    .bind(episode_one_id.to_string())
    .bind(series_id.to_string())
    .bind(season_id.to_string())
    .bind(episode_two_id.to_string())
    .execute(pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO media_files (id, media_item_id, path, scan_state) \
         VALUES ($1, $2, $3, 'ok')",
    )
    .bind(media_file_id.to_string())
    .bind(series_id.to_string())
    .bind(path)
    .execute(pool)
    .await?;
    for episode_id in [episode_one_id, episode_two_id] {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO episode_files (episode_id, media_file_id) VALUES ($1, $2)",
        )
        .bind(episode_id.to_string())
        .bind(media_file_id.to_string())
        .execute(pool)
        .await?;
    }

    let release_id = Uuid::new_v4();
    let release_job_id = Uuid::new_v4();
    let import_run_id = Uuid::new_v4();
    sqlx::query::<sqlx::Any>(
        "INSERT INTO acquisition_releases (\
            release_id, source_extension_id, media_type, title, release_title, source, \
            source_kind, fingerprint, release_kind, resolver_kind, resolver_version, confidence\
         ) VALUES ($1, 'fixture.source', 'anime', 'Tokyo Ghoul', \
                   'Tokyo Ghoul Root A 01-02', 'fixture', 'torrent', $2, 'season_pack', \
                   'deterministic', '1', 'verified')",
    )
    .bind(release_id.to_string())
    .bind(format!("alm8-pack-{release_id}"))
    .execute(pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO acquisition_release_jobs (\
            release_job_id, release_id, route_logical_id, state\
         ) VALUES ($1, $2, 'fixture.route', 'completed')",
    )
    .bind(release_job_id.to_string())
    .bind(release_id.to_string())
    .execute(pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO acquisition_import_runs (\
            import_run_id, release_id, release_job_id, route_logical_id, state\
         ) VALUES ($1, $2, $3, 'fixture.route', 'completed')",
    )
    .bind(import_run_id.to_string())
    .bind(release_id.to_string())
    .bind(release_job_id.to_string())
    .execute(pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO acquisition_import_file_links (\
            import_link_id, import_run_id, release_id, local_path, media_file_id, \
            episode_id, state, verification_state\
         ) VALUES ($1, $2, $3, $4, $5, $6, 'imported', 'verified')",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(import_run_id.to_string())
    .bind(release_id.to_string())
    .bind(path)
    .bind(media_file_id.to_string())
    .bind(episode_one_id.to_string())
    .execute(pool)
    .await?;

    Ok((media_file_id, episode_one_id, episode_two_id))
}

#[tokio::test]
async fn alm8_authoritative_multi_episode_pack_is_never_collapsed_by_model_matching() -> Result<()>
{
    let database = alm8_database().await?;
    persist_cached_anizip_mapping(
        &database.pool,
        "1002",
        &tokyo_ghoul_root_a_two_episode_mapping(),
    )
    .await?;
    let dir = tempdir()?;
    let path = dir
        .path()
        .join("Tokyo Ghoul Root A 01-02.mkv")
        .to_string_lossy()
        .to_string();
    std::fs::write(&path, b"alm8-authoritative-pack")?;
    let (media_file_id, episode_one_id, episode_two_id) =
        seed_authoritative_two_episode_pack(&database.pool, &path).await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let matcher = AnimeMatchingService::with_engine(Arc::new(LibraryMatchEngine {
        calls: calls.clone(),
        requests: Arc::new(Mutex::new(Vec::new())),
    }));

    scan_with_matcher(&database, &matcher, anime_candidate(&path, None, None)).await?;

    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "verified acquisition packs must not be reduced to the model's single-target contract"
    );
    let linked_ids: Vec<String> = sqlx::query_scalar(
        "SELECT episode_id FROM episode_files WHERE media_file_id = $1 ORDER BY episode_id",
    )
    .bind(media_file_id.to_string())
    .fetch_all(&database.pool)
    .await?;
    let mut expected = vec![episode_one_id.to_string(), episode_two_id.to_string()];
    expected.sort_unstable();
    assert_eq!(linked_ids, expected);
    let disposition: String = sqlx::query_scalar(
        "SELECT disposition FROM classifier_resolution_state WHERE media_file_id = $1",
    )
    .bind(media_file_id.to_string())
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(disposition, "applied");
    assert!(std::path::Path::new(&path).is_file());
    Ok(())
}

#[tokio::test]
async fn alm8_episode_link_and_classification_roll_back_as_one_transaction() -> Result<()> {
    let database = alm8_database().await?;
    let series_id = Uuid::new_v4();
    let season_id = Uuid::new_v4();
    let old_episode_id = Uuid::new_v4();
    let target_episode_id = Uuid::new_v4();
    let media_file_id = Uuid::new_v4();
    sqlx::query::<sqlx::Any>(
        "INSERT INTO media_items (id, type, title) VALUES ($1, 'anime', 'Atomic Anime')",
    )
    .bind(series_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO series (id, title, library_type) VALUES ($1, 'Atomic Anime', 'anime')",
    )
    .bind(series_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO seasons (id, series_id, season_number) VALUES ($1, $2, 2)",
    )
    .bind(season_id.to_string())
    .bind(series_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO episodes \
         (id, series_id, season_id, season_number, episode_number, \
          absolute_episode_number, has_file) \
         VALUES ($1, $2, $3, 2, 1, 13, TRUE), ($4, $2, $3, 2, 2, 14, FALSE)",
    )
    .bind(old_episode_id.to_string())
    .bind(series_id.to_string())
    .bind(season_id.to_string())
    .bind(target_episode_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO media_files (id, media_item_id, path, scan_state) \
         VALUES ($1, $2, '/media/alm8-atomic.mkv', 'ok')",
    )
    .bind(media_file_id.to_string())
    .bind(series_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO episode_files (episode_id, media_file_id) VALUES ($1, $2)",
    )
    .bind(old_episode_id.to_string())
    .bind(media_file_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "CREATE TRIGGER alm8_classification_failure \
         BEFORE INSERT ON classifier_resolution_state \
         BEGIN SELECT RAISE(ABORT, 'forced ALM-8 classification failure'); END",
    )
    .execute(&database.pool)
    .await?;
    let outcome = ClassificationOutcome {
        disposition: ClassificationDisposition::Applied,
        confidence: Some(1.0),
        hint_json: None,
        candidates_json: Some(
            json!({
                "animeMatchAssist": {
                    "source": "local_model",
                    "result": "matched",
                    "matcherSchemaVersion": ANIME_MATCH_SCHEMA_VERSION,
                    "latencyMs": 1
                }
            })
            .to_string(),
        ),
        season_scope: Some(2),
        retry_supersedes_applied: true,
        bridge_protected: false,
        parsed_hint: None,
        accepted_numbers: Some(ResolvedEpisodeNumbers {
            season: Some(2),
            episode: Some(2),
            absolute_episode: Some(14),
        }),
        preserve_authoritative_episode_links: false,
        applied_identity_rows: Default::default(),
    };

    let error = link_episode_file_with_classification(
        &database.pool,
        target_episode_id,
        media_file_id,
        &outcome,
    )
    .await
    .expect_err("classification trigger must abort the complete link transaction");
    assert!(
        error
            .to_string()
            .contains("forced ALM-8 classification failure"),
        "unexpected transaction error: {error:#}"
    );

    let linked_ids: Vec<String> = sqlx::query_scalar(
        "SELECT episode_id FROM episode_files WHERE media_file_id = $1 ORDER BY episode_id",
    )
    .bind(media_file_id.to_string())
    .fetch_all(&database.pool)
    .await?;
    assert_eq!(linked_ids, vec![old_episode_id.to_string()]);
    let classification_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM classifier_resolution_state WHERE media_file_id = $1",
    )
    .bind(media_file_id.to_string())
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(classification_count, 0);
    let file_states: Vec<(String, i64)> = sqlx::query_as(
        "SELECT id, CAST(has_file AS INTEGER) FROM episodes \
         WHERE id IN ($1, $2) ORDER BY id",
    )
    .bind(old_episode_id.to_string())
    .bind(target_episode_id.to_string())
    .fetch_all(&database.pool)
    .await?;
    let old_episode_id = old_episode_id.to_string();
    let target_episode_id = target_episode_id.to_string();
    let old_has_file = file_states
        .iter()
        .find(|(id, _)| id == &old_episode_id)
        .map(|(_, has_file)| *has_file);
    let target_has_file = file_states
        .iter()
        .find(|(id, _)| id == &target_episode_id)
        .map(|(_, has_file)| *has_file);
    assert_eq!(old_has_file, Some(1));
    assert_eq!(target_has_file, Some(0));
    Ok(())
}
